#include <boost/json.hpp>
#include <iomanip>
#include <iostream>

namespace json = boost::json;

inline json::value parse_file(std::istream &sm) {
  json::stream_parser p;
  json::error_code ec;
  char buf[4096];
  do {
    sm.read(buf, sizeof(buf));
    size_t size = sm.gcount();
    if (size == 0) {
      break;
    }
    p.write(buf, size, ec);
  } while (!sm.eof());
  if (ec)
    return nullptr;
  p.finish(ec);
  if (ec)
    return nullptr;
  return p.release();
}

inline void
pretty_print(std::ostream &os, json::value const &jv, std::string *indent = nullptr) {
  std::string indent_;
  if (!indent)
    indent = &indent_;
  switch (jv.kind()) {
  case json::kind::object: {
    os << "{\n";
    indent->append(4, ' ');
    auto const &obj = jv.get_object();
    if (!obj.empty()) {
      auto it = obj.begin();
      for (;;) {
        os << *indent << json::serialize(it->key()) << " : ";
        pretty_print(os, it->value(), indent);
        if (++it == obj.end())
          break;
        os << ",\n";
      }
    }
    os << "\n";
    indent->resize(indent->size() - 4);
    os << *indent << "}";
    break;
  }

  case json::kind::array: {
    os << "[\n";
    indent->append(4, ' ');
    auto const &arr = jv.get_array();
    if (!arr.empty()) {
      auto it = arr.begin();
      for (;;) {
        os << *indent;
        pretty_print(os, *it, indent);
        if (++it == arr.end())
          break;
        os << ",\n";
      }
    }
    os << "\n";
    indent->resize(indent->size() - 4);
    os << *indent << "]";
    break;
  }

  case json::kind::string: {
    os << json::serialize(jv.get_string());
    break;
  }

  case json::kind::uint64:
    \
os << jv.get_uint64();
    break;

  case json::kind::int64:os << jv.get_int64();
    break;

  case json::kind::double_:os << jv.get_double();
    break;

  case json::kind::bool_:
    if (jv.get_bool())
      os << "true";
    else
      os << "false";
    break;

  case json::kind::null:os << "null";
    break;
  }

  if (indent->empty())
    os << "\n";
}

inline int json_pretty(std::istream &is, std::ostream &os) {
  try {
    // Parse the file as JSON
    auto const jv = parse_file(is);

// Now pretty-print the value
    pretty_print(os, jv);
  }
  catch (std::exception const &e) {
    std::cerr <<
              "Caught exception: "
              << e.what() << std::endl;
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}

inline std::string json_pretty(const std::string &json) {
  std::stringstream is;
  std::stringstream os;
  is << json;
  json_pretty(is, os);
  return os.str();
}