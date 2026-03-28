$c = Get-Content -Encoding UTF8 src\rpc\include\mprpcchannel.h -Raw
$c = $c.Replace("const uint16_t m_port;", "const uint16_t m_port;`n  std::mutex m_clientFd_mutex;")
$c = $c.Replace("#include <string>", "#include <string>`n#include <mutex>")
Set-Content -Encoding UTF8 src\rpc\include\mprpcchannel.h $c

$c = Get-Content -Encoding UTF8 src\rpc\mprpcchannel.cpp -Raw
$c = $c.Replace("m_clientFd = clientfd;", "  struct timeval tv;`n  tv.tv_sec = 0;`n  tv.tv_usec = 500000; // 500ms timeout`n  setsockopt(clientfd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);`n  setsockopt(clientfd, SOL_SOCKET, SO_SNDTIMEO, (const char*)&tv, sizeof tv);`n  m_clientFd = clientfd;")
$c = $c.Replace("void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,`r`n                              google::protobuf::RpcController* controller, const google::protobuf::Message* request,`r`n                              google::protobuf::Message* response, google::protobuf::Closure* done) {", "void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,`r`n                              google::protobuf::RpcController* controller, const google::protobuf::Message* request,`r`n                              google::protobuf::Message* response, google::protobuf::Closure* done) {`n  std::lock_guard<std::mutex> lock(m_clientFd_mutex);")
Set-Content -Encoding UTF8 src\rpc\mprpcchannel.cpp $c
