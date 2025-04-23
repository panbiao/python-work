import http.client
import json

def get_api_response(prompt, model="gpt-3.5-turbo", temperature=0.7):
    # 代理服务器地址和端口
    proxy_host = "127.0.0.1"
    proxy_port = 10809

    # 目标主机和端点
    target_host = "api.openai.com"
    endpoint = "/v1/chat/completions"

    # 创建连接到代理服务器
    conn = http.client.HTTPConnection(proxy_host, proxy_port)

    # 请求头
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer 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",
        "Host": target_host  # 告诉目标主机
    }

    # 请求体
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperature
    }

    # 发送请求，路径中包含完整的目标 URL
    conn.request("POST", f"https://{target_host}{endpoint}", body=json.dumps(payload), headers=headers)

    # 获取响应
    response = conn.getresponse()
    data = response.read()
    conn.close()

    # 检查响应状态码
    if response.status == 200:
        return json.loads(data.decode("utf-8"))
    else:
        print(f"Error: {response.status} - {response.reason}")
        print(data.decode("utf-8"))
        return None

    
if __name__ == "__main__":
    prompt = "特朗普是个什么样的人?"
    response = get_api_response(prompt)
    if response:
        print(json.dumps(response, indent=2))
        print("Response:", response['choices'][0]['message']['content'])
