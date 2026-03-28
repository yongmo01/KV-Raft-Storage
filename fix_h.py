import sys

with open(r"src\rpc\include\mprpcchannel.h", "r", encoding="utf-8") as f:
    text = f.read()

if "<mutex>" not in text:
    text = text.replace("#include <string>", "#include <string>\n#include <mutex>")

if "std::mutex m_clientFd_mutex;" not in text:
    text = text.replace("const uint16_t m_port;", "const uint16_t m_port;\n  std::mutex m_clientFd_mutex;")

with open(r"src\rpc\include\mprpcchannel.h", "w", encoding="utf-8") as f:
    f.write(text)
