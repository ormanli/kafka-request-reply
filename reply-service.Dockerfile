FROM golang:1.20 as builder
RUN mkdir kafka-request-reply
WORKDIR kafka-request-reply

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo ./reply-service/main.go

FROM scratch
COPY --from=builder /go/kafka-request-reply/main ./main
CMD ["./main"]