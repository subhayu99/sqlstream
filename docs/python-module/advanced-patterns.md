# Advanced Usage

## Backend Selection

You can explicitly choose the execution backend to optimize for your specific use case.

```python
# Force pure Python backend (streaming, low memory)
query("large_file.csv").sql("SELECT *", backend="python")

# Force Pandas backend (fast, high memory)
query("data.csv").sql("SELECT *", backend="pandas")
```

## S3 Configuration

To access S3, ensure you have `s3fs` installed and your AWS credentials configured.

```bash
pip install sqlstream[s3]
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

Then simply use `s3://` paths.

## Custom Readers

You can implement custom readers by inheriting from `BaseReader`. This allows you to support proprietary formats or custom data sources.

```python
from sqlstream.readers.base import BaseReader

class MyCustomReader(BaseReader):
    def read_lazy(self):
        yield {"col": "value"}
```
