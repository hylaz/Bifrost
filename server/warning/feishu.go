package warning

import (
	"encoding/json"
	"github.com/sirupsen/logrus"
	"net/http"
	"strings"
)

type Feishu struct {
	p FeishuParam
}

type FeishuParam struct {
	Webhook string `json:"webhook"`
}

type PostData struct {
	MsgType string `json:"msg_type"`
	Content struct {
		Text string `json:"text"`
	} `json:"content"`
}

func init() {
	Register("Feishu", &Feishu{})
}

func (This *Feishu) paramTansfer(p map[string]interface{}) error {
	s, err := json.Marshal(p)
	if err != nil {
		return err
	}
	err = json.Unmarshal(s, &This.p)
	if err != nil {
		return err
	}
	return nil
}

func (This *Feishu) SendWarning(p map[string]interface{}, title string, Body string) error {
	err := This.paramTansfer(p)
	if err != nil {
		return err
	}

	data := PostData{}
	data.MsgType = "text"
	data.Content.Text = Body

	b, err := json.Marshal(data)
	if err != nil {
		logrus.Println("sendToWeChatMsg json.Marshal err:", err)
		return err
	}
	return sendFeishuMsg(This.p.Webhook, string(b))
}

func sendFeishuMsg(url string, data string) error {
	resp, err := http.Post(url, "application/json", strings.NewReader(data))
	if err != nil {
		return err
	}
	var res map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&res)
	return nil
}
