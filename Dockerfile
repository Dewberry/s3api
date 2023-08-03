FROM osgeo/gdal:alpine-small-3.6.2 AS dev

COPY --from=golang:1.19-alpine3.15 /usr/local/go/ /usr/local/go/

ENV PATH="/usr/local/go/bin:/root/go/bin:${PATH}"

RUN apk add --no-cache git alpine-sdk

RUN go install github.com/githubnemo/CompileDaemon@v1.4.0

WORKDIR /app
COPY . .

RUN go mod tidy && go build main.go

ENTRYPOINT CompileDaemon --build="go build main.go" --command="./main"

FROM osgeo/gdal:alpine-small-3.6.2 AS prod

COPY --from=dev /app/main /app/main
EXPOSE 5000
CMD ["/app/main" ]