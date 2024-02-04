# Badger Store

## Tables

Since BadgerDB has a single namespace, we use key prefixes to designate separate
namespaces. These namespaces act as tables or indexes. Below are the namespaces
that are used internally, along with their single-byte prefixes:

| Prefix | Namespace | Description |
|--------|-----------|-------------|
| `0x00` | Idents | Holds covering index of (ID, name) for each ident |
| `0x01` | IdentIDByName | Mapping of Ident name to Ident ID |
| `0x02` | EAVT | (Entity, Attribute) -> (Value, Tx) |
| `0x03` | AEVT | (Attribute, Entity) -> (Value, Tx) |
| `0x04` | AVET | (Attribute, Value) -> (Entity, Tx) |
| `0x05` | VAET | (Value, Attribute) -> (Entity, Tx) |

## Ident Storage

Since Badger keeps keys in LSM trees, and there should always be a small number
of idents, we keep all idents in a covering index whose key encodes both the ID
and name of each ident. This should allow us to look up any ident by name
extremely quickly. Additionally, we keep a separate index of `(ID, Name)`, but
in this case, the name is stored as a value and will not be kept in memory.
Thus, ident lookups by ID will be slower, but this will not be a common
operation.
