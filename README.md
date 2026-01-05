group                                                                                 improve-row-lengths-for-binary         main
-----                                                                                 ------------------------------         ----
append_rows 4096 string(10, 0)                                                        1.00     36.1±0.13µs        ? ?/sec    1.35     48.8±0.37µs        ? ?/sec
append_rows 4096 string(100, 0)                                                       1.00     68.7±2.88µs        ? ?/sec    1.04     71.3±1.64µs        ? ?/sec
append_rows 4096 string(100, 0.5)                                                     1.11     91.1±2.23µs        ? ?/sec    1.00     81.9±1.48µs        ? ?/sec
append_rows 4096 string(20, 0.5), string(30, 0), string(100, 0), i64(0)               1.00    211.5±3.56µs        ? ?/sec    1.04    220.6±3.80µs        ? ?/sec
append_rows 4096 string(30, 0)                                                        1.00     39.1±0.50µs        ? ?/sec    1.27     49.5±0.47µs        ? ?/sec

convert_columns 4096 string(10, 0)                                                    1.00     36.5±0.34µs        ? ?/sec    1.33     48.7±0.45µs        ? ?/sec
convert_columns 4096 string(100, 0)                                                   1.00     69.3±0.97µs        ? ?/sec    1.04     72.0±0.92µs        ? ?/sec
convert_columns 4096 string(100, 0.5)                                                 1.11     91.1±0.83µs        ? ?/sec    1.00     82.0±1.25µs        ? ?/sec
convert_columns 4096 string(20, 0.5), string(30, 0), string(100, 0), i64(0)           1.00    213.1±3.04µs        ? ?/sec    1.04    221.4±2.26µs        ? ?/sec
convert_columns 4096 string(30, 0)                                                    1.00     39.4±0.39µs        ? ?/sec    1.26     49.6±0.22µs        ? ?/sec

convert_columns_prepared 4096 string(10, 0)                                           1.00     36.2±0.46µs        ? ?/sec    1.34     48.6±0.33µs        ? ?/sec
convert_columns_prepared 4096 string(100, 0)                                          1.00     68.7±0.84µs        ? ?/sec    1.04     71.8±0.83µs        ? ?/sec
convert_columns_prepared 4096 string(100, 0.5)                                        1.11     91.0±0.89µs        ? ?/sec    1.00     81.9±0.35µs        ? ?/sec
convert_columns_prepared 4096 string(20, 0.5), string(30, 0), string(100, 0), i64(0)  1.00    210.5±1.60µs        ? ?/sec    1.05    220.9±3.37µs        ? ?/sec
convert_columns_prepared 4096 string(30, 0)                                           1.00     39.3±0.45µs        ? ?/sec    1.26     49.5±0.35µs        ? ?/sec

convert_rows 4096 string(10, 0)                                                       1.07     64.8±0.19µs        ? ?/sec    1.00     60.4±1.39µs        ? ?/sec
convert_rows 4096 string(100, 0)                                                      1.04    114.6±1.40µs        ? ?/sec    1.00    110.2±0.71µs        ? ?/sec
convert_rows 4096 string(100, 0.5)                                                    1.05    108.4±1.08µs        ? ?/sec    1.00    103.4±0.51µs        ? ?/sec
convert_rows 4096 string(20, 0.5), string(30, 0), string(100, 0), i64(0)              1.04   315.1±12.59µs        ? ?/sec    1.00   304.2±13.21µs        ? ?/sec
convert_rows 4096 string(30, 0)                                                       1.08     78.3±3.33µs        ? ?/sec    1.00     72.6±0.96µs        ? ?/sec
