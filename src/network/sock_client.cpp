#include "network/client.h"

void client::connected(ptr<tcp::socket> sock, message_handler handler) {
  connection::connected(sock, handler);
}
