# Download pg source code
# Build
  
configure with coverage

    # configure enable coverage
    ./configure --enable-coverage
  
edit src/test/isolation/isolation_schedule

remove all lines except "deadlock*"

run make test, in src/test/isolation/, run:

    make check

in project root path, run:

    make coverage-html

and in coverage folder, we would get the coverage result.
