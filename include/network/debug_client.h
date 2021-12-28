#pragma once

#include <string>
#include <iostream>

int debug_request(
    const std::string &address,
    uint16_t port,
    const std::string &path, std::ostream &os);