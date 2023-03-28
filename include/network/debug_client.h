#pragma once

#include <iostream>
#include <string>

int debug_request(const std::string &address, uint16_t port,
                  const std::string &path, std::ostream &os);