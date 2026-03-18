package service

import (
    "context"
    "log"

    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/metadata"
    "google.golang.org/grpc/status"
)

type AuthInterceptor struct {
    apiKey string
}

func NewAuthInterceptor(apiKey string) *AuthInterceptor {
    return &AuthInterceptor{apiKey: apiKey}
}

func (a *AuthInterceptor) Unary() grpc.UnaryServerInterceptor {
    return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
        if a.apiKey == "" {
            return handler(ctx, req)
        }
        md, ok := metadata.FromIncomingContext(ctx)
        if !ok {
            log.Println("AUTH: no metadata")
            return nil, status.Error(codes.Unauthenticated, "missing metadata")
        }
        keys := md.Get("x-api-key")
        if len(keys) == 0 {
            log.Println("AUTH: no x-api-key")
            return nil, status.Error(codes.Unauthenticated, "missing api key")
        }
        if keys[0] != a.apiKey {
            log.Println("AUTH: invalid api key")
            return nil, status.Error(codes.Unauthenticated, "invalid api key")
        }
        return handler(ctx, req)
    }
}
