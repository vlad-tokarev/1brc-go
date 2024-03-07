
### About

Solution in Go for [1BRC challenge](https://github.com/gunnarmorling/1brc)

Optimisations:

- Custom float64 parser (to avoid string allocations)
- Use fixed size [N]byte array for map keys (to avoid string allocations)
- Parallelization: map-reduce approach

### Performance

Machine:

- Chip: Apple M1 Max
- Total Number of Cores: 10 (8 performance and 2 efficiency)
- Memory: 32 GB

Results:

- 1 Goroutine: took 1m18.318s
- 10 Goroutines: took 13.077s
