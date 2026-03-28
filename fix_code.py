import sys

with open(r"src\rpc\mprpcchannel.cpp", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace("m_clientFd = clientfd;", """
  // Set timeout to prevent indefinitely blocking on recv
  struct timeval tv;
  tv.tv_sec = 0;
  tv.tv_usec = 500000; // 500ms
  setsockopt(clientfd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);
  setsockopt(clientfd, SOL_SOCKET, SO_SNDTIMEO, (const char*)&tv, sizeof tv);
  m_clientFd = clientfd;""")

# Add mutex logic at the top of CallMethod 
text = text.replace("void MprpcChannel::CallMethod(", """
void MprpcChannel::CallMethod(""")

if "std::lock_guard<std::mutex> lock(m_clientFd_mutex);" not in text:
    text = text.replace("void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,\n                              google::protobuf::RpcController* controller, const google::protobuf::Message* request,\n                              google::protobuf::Message* response, google::protobuf::Closure* done) {",
"""void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                              google::protobuf::RpcController* controller, const google::protobuf::Message* request,
                              google::protobuf::Message* response, google::protobuf::Closure* done) {
  std::lock_guard<std::mutex> lock(m_clientFd_mutex);""")


with open(r"src\rpc\mprpcchannel.cpp", "w", encoding="utf-8") as f:
    f.write(text)
