# Musashi regression fixture

`musashi-ca89430c.hex` is the unmodified CBOR hex supplied by Rodrigo for
[issue #24](https://github.com/SundaeSwap-finance/sundae-go-utils/issues/24).
It is a Dijkstra **block transaction**, encoded as
`[body, witnesses, auxiliary_data, is_valid]`, not a standalone mempool transaction.

- Top-level ID: `ca89430ca01c3202ce2cc8428c53f91a4f34d618421ca510dd39f3d76c540aca`
- Sub-transaction ID: `ede49f69c8e99d253d82b0b06d024be6435168cfeb1b960761370fe1bf5f8377`
- The sub-transaction produces outputs `#0` (an order with an inline datum) and
  `#1`; the top level produces its own `#0`.
- The bodies each have one input, spending indices `3` and `2`, respectively,
  of `5358e654e79a4f956cfceca25be30a55aaa9b2df5599af1b8d222786fc4a41a0`.
- Each level has its own key witness. The fixture contains no witness datums
  or redeemers; tests add distinct synthetic ones where needed.

Replay tests wrap these original transaction bytes in a synthetic block header.
They exercise archive decoding and callback dispatch, not chain validation or
the original block's hash/slot.
