package sundaegql

import (
	"fmt"
	"net/http"

	"github.com/SundaeSwap-finance/sundae-go-utils/graphiql"
	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/graph-gophers/graphql-go"
	"github.com/graph-gophers/graphql-go/relay"
	"github.com/rs/zerolog"
	"github.com/savaki/apigateway"
)

// Serve a mildly opinionated graphql webserver, optionally with playground attached
func Webserver(resolver Resolver) error {
	config := resolver.Config()
	relay, err := GraphQLRelay(resolver)
	if err != nil {
		return err
	}

	router := DefaultRouter(config.Logger)

	router.Post("/graphql", middleware.NoCache(relay).ServeHTTP)
	// Allow arbitrary path parameters, for better UX in the browser
	router.Post("/graphql/*", middleware.NoCache(relay).ServeHTTP)
	if AllowIntrospection() {
		path := "/graphql"
		if config.Service.Subpath != "" {
			path = fmt.Sprintf("/%v/graphql", config.Service.Subpath)
		}
		router.Get("/graphql", graphiql.New(path))
	}

	return Serve(router, config)
}

// Construct an http relay that handles graphql requests
func GraphQLRelay(resolver Resolver) (*relay.Handler, error) {
	finalSchema := resolver.Schema()

	config := resolver.Config()
	config.Service.Schema = finalSchema

	opts := []graphql.SchemaOpt{
		graphql.MaxDepth(15),
		graphql.UseFieldResolvers(),
	}
	if !AllowIntrospection() {
		opts = append(opts, graphql.DisableIntrospection())
	}

	schema, err := graphql.ParseSchema(finalSchema, resolver, opts...)
	if err != nil {
		return nil, fmt.Errorf("unable to parse schema: %w", err)
	}

	return &relay.Handler{Schema: schema}, nil
}

// Construct a chi router with the common useful middleware
func DefaultRouter(logger zerolog.Logger) chi.Router {
	router := chi.NewRouter()
	router.Use(
		middleware.Logger,
		WithCORS(),
		WithLogger(logger),
		middleware.Recoverer,
	)
	return router
}

// Start listening / serving a graphql server, or as a Lambda function
func Serve(router chi.Router, config *BaseConfig) error {
	if sundaecli.CommonOpts.Console {
		config.Logger.Info().Int("port", sundaecli.CommonOpts.Port).Msgf("starting %v", config.Service.Name)
		addr := fmt.Sprintf(":%v", sundaecli.CommonOpts.Port)
		if config.Service.Subpath != "" {
			newRouter := chi.NewRouter()
			newRouter.Mount(fmt.Sprintf("/%v", config.Service.Subpath), router)
			router = newRouter
		}
		return http.ListenAndServe(addr, router)
	}

	lambda.Start(apigateway.Wrap(router, sundaecli.CommonOpts.Env, config.Service.Subpath))
	return nil
}
