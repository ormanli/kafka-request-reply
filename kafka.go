package kafka

const TopicHelloRequest = "hello_request"
const TopicHelloReply = "hello_reply"
const HeaderReplyTopic = "reply_topic"
const HeaderReplyPartition = "reply_partition"
const HeaderReplyTo = "reply_to"

type RequestMessage struct {
	Name string `json:"name"`
}

type ReplyMessage struct {
	Says string `json:"says"`
}
