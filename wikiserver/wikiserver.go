/*
 *
 * Copyright 2023 puzzlewikiserver authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package wikiserver

import (
	"context"
	"errors"

	mongoclient "github.com/dvaumoron/puzzlemongoclient"
	pb "github.com/dvaumoron/puzzlewikiservice"
	"github.com/uptrace/opentelemetry-go-extra/otelzap"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

const WikiKey = "puzzleWiki"

const collectionName = "pages"

const wikiIdKey = "wikiId"
const wikiRefKey = "ref"
const versionKey = "version"
const textKey = "text"
const userIdKey = "userId"

const mongoCallMsg = "Failed during MongoDB call"

var errInternal = errors.New("internal service error")

var descVersion = bson.D{{Key: versionKey, Value: -1}}
var contentFields = bson.D{
	// exclude unused fields
	{Key: wikiIdKey, Value: false}, {Key: wikiRefKey, Value: false}, {Key: userIdKey, Value: false},
}
var optsContentMaxVersion = options.FindOne().SetSort(descVersion).SetProjection(contentFields)
var optsContentFields = options.FindOne().SetProjection(contentFields)
var optsVersion = options.Find().SetProjection(
	bson.D{{Key: versionKey, Value: true}, {Key: userIdKey, Value: true}},
)

// server is used to implement puzzlewikiservice.WikiServer
type server struct {
	pb.UnimplementedWikiServer
	clientOptions *options.ClientOptions
	databaseName  string
	logger        *otelzap.Logger
}

func New(clientOptions *options.ClientOptions, databaseName string, logger *otelzap.Logger) pb.WikiServer {
	return server{clientOptions: clientOptions, databaseName: databaseName, logger: logger}
}

func (s server) Load(ctx context.Context, request *pb.WikiRequest) (*pb.Content, error) {
	logger := s.logger.Ctx(ctx)
	client, err := mongo.Connect(ctx, s.clientOptions)
	if err != nil {
		logger.Error(mongoCallMsg, zap.Error(err))
		return nil, errInternal
	}
	defer mongoclient.Disconnect(client, logger)

	collection := client.Database(s.databaseName).Collection(collectionName)

	filters := bson.D{
		{Key: wikiIdKey, Value: request.WikiId}, {Key: wikiRefKey, Value: request.WikiRef},
	}

	opts := optsContentMaxVersion
	if version := request.Version; version != 0 {
		filters = append(filters, bson.E{Key: versionKey, Value: version})
		opts = optsContentFields
	}

	var result bson.M
	err = collection.FindOne(ctx, filters, opts).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// an empty Content has Version 0, which is recognized by client
			return &pb.Content{}, nil
		}

		logger.Error(mongoCallMsg, zap.Error(err))
		return nil, errInternal
	}
	return convertToContent(result), nil
}

func (s server) Store(ctx context.Context, request *pb.ContentRequest) (*pb.Response, error) {
	logger := s.logger.Ctx(ctx)
	client, err := mongo.Connect(ctx, s.clientOptions)
	if err != nil {
		logger.Error(mongoCallMsg, zap.Error(err))
		return nil, errInternal
	}
	defer mongoclient.Disconnect(client, logger)

	collection := client.Database(s.databaseName).Collection(collectionName)

	// rely on the mongo server to ensure there will be no duplicate
	newVersion := request.Last + 1
	page := bson.M{
		wikiIdKey: request.WikiId, wikiRefKey: request.WikiRef, versionKey: newVersion,
		userIdKey: request.UserId, textKey: request.Text,
	}

	_, err = collection.InsertOne(ctx, page)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return &pb.Response{}, nil
		}

		logger.Error(mongoCallMsg, zap.Error(err))
		return nil, errInternal
	}
	return &pb.Response{Success: true}, nil
}

func (s server) ListVersions(ctx context.Context, request *pb.VersionRequest) (*pb.Versions, error) {
	logger := s.logger.Ctx(ctx)
	client, err := mongo.Connect(ctx, s.clientOptions)
	if err != nil {
		logger.Error(mongoCallMsg, zap.Error(err))
		return nil, errInternal
	}
	defer mongoclient.Disconnect(client, logger)

	collection := client.Database(s.databaseName).Collection(collectionName)

	cursor, err := collection.Find(ctx, bson.D{
		{Key: wikiIdKey, Value: request.WikiId}, {Key: wikiRefKey, Value: request.WikiRef},
	}, optsVersion)
	if err != nil {
		logger.Error(mongoCallMsg, zap.Error(err))
		return nil, errInternal
	}

	var results []bson.M
	if err = cursor.All(ctx, &results); err != nil {
		logger.Error(mongoCallMsg, zap.Error(err))
		return nil, errInternal
	}
	return &pb.Versions{List: mongoclient.ConvertSlice(results, convertToVersion)}, nil
}

func (s server) Delete(ctx context.Context, request *pb.WikiRequest) (*pb.Response, error) {
	logger := s.logger.Ctx(ctx)
	client, err := mongo.Connect(ctx, s.clientOptions)
	if err != nil {
		logger.Error(mongoCallMsg, zap.Error(err))
		return nil, errInternal
	}
	defer mongoclient.Disconnect(client, logger)

	collection := client.Database(s.databaseName).Collection(collectionName)

	_, err = collection.DeleteMany(ctx, bson.D{
		{Key: wikiIdKey, Value: request.WikiId}, {Key: wikiRefKey, Value: request.WikiRef},
		{Key: versionKey, Value: request.Version},
	})
	if err != nil {
		logger.Error(mongoCallMsg, zap.Error(err))
		return nil, errInternal
	}
	return &pb.Response{Success: true}, nil
}

func convertToContent(page bson.M) *pb.Content {
	text, _ := page[textKey].(string)
	return &pb.Content{
		Version: mongoclient.ExtractUint64(page[versionKey]),
		Text:    text, CreatedAt: mongoclient.ExtractCreateDate(page).Unix(),
	}
}

func convertToVersion(page bson.M) *pb.Version {
	return &pb.Version{
		Number: mongoclient.ExtractUint64(page[versionKey]),
		UserId: mongoclient.ExtractUint64(page[userIdKey]),
	}
}
