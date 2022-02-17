# strip-alma-user-stats

This is a console application that pulls Alma users and removes User Statistics from them based on a provided list of categories.

    strip-alma-user-stats 0.1.0

    USAGE:
        strip-alma-user-stats.exe [OPTIONS] <categories-file>

    FLAGS:
        -h, --help       Prints help information
        -V, --version    Prints version information

    OPTIONS:
        -f, --from-offset <from-offset>     [default: 0]
        -t, --to-offset <to-offset>

    ARGS:
        <categories-file>

The users are pulled in batches using the Alma API's maximum page limit of 100. The `from-offset` and `to-offset` options allow specification of which user batches to update, and are inclusive.

In the categories file, each category identifier is expected to be on its own line.

The connection to Alma is configured with `ALMA_REGION` and `ALMA_APIKEY` environment variables, and the `RUST_LOG` environment variable can be used to configure the log level.