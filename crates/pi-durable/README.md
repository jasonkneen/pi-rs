# pi-durable

Native Rust services for durable conversations, tasks and document storage. The
port is in progress; the complete acceptance scope and remaining gaps are in
[the port status](../../porting/durable/STATUS.md).

Run an offline conversation through the native generation task, scheduler,
submission controller and model SDK:

```sh
cargo run -p pi-durable --example conversation -- "Hello"
```

The example uses the SDK's scripted faux provider and memory storage. It prints
the settled submission and committed conversation messages. It needs no API key
or JavaScript runtime. Its registry installs native generation, tool and
compaction definitions; `SchedulerOptions::models` supplies the model collection.

Generation supports prompt preparation, pinned requests, trailing partial
writes, retries, deferred polling/cancellation, tool rounds and run settlement.
The default provider boundary currently uses the existing typed Rust SDK.
Lossless SDK 1.4 alignment, arbitrary hosted model values, complete host callback
bindings and the combined public Harness API remain open.

The port gate runs native tests and pinned stock pi-mono tests against Rust:

```sh
bash scripts/check-durable-port.sh /path/to/pi-mono
```

Stock selection, host capabilities and unselected cases are recorded explicitly
in the status and generated test reports.
