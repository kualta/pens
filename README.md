# pretty-ens

Check availability of .eth and .box domains from a wordlist.

## Install

```bash
cargo install pretty-ens
```

## Usage

```bash
# Fetch a wordlist
pretty-ens --fetch-preset bip39 -o words.txt
pretty-ens --list-presets

# Run checker
pretty-ens -w words.txt --eth
pretty-ens -w words.txt --box
pretty-ens -w words.txt
pretty-ens -w words.txt --resume

# View results
pretty-ens --status
pretty-ens --show
pretty-ens --show --eth
pretty-ens --show --both

# Export
pretty-ens --export -o results.csv
pretty-ens --export -o results.csv --filter available
```

## Options

- `-w, --wordlist` - wordlist file
- `-c, --checkpoint` - checkpoint file (default: checkpoint.json)
- `-o, --output` - output file for fetch/export
- `--concurrency` - concurrent requests (default: 5)
- `--eth-rps` - ETH RPC rate limit (default: 5)
- `--box-rps` - RDAP rate limit (default: 2)
