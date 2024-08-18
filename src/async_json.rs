use serde::Serialize;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

pub struct ArrayWriter<W> {
    buffer: Vec<u8>,
    following_item: bool,
    writer: W,
}

impl<W> ArrayWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            following_item: false,
            buffer: Vec::with_capacity(10 * 1024),
            writer,
        }
    }
}

impl<W> ArrayWriter<W>
where
    W: AsyncWrite + Unpin,
{
    pub async fn write_open_bracket(&mut self) -> anyhow::Result<()> {
        self.writer.write_all(b"[\n").await?;
        Ok(())
    }

    pub async fn write_close_bracket(&mut self) -> anyhow::Result<()> {
        self.writer.write_all(b"\n]\n").await?;
        Ok(())
    }

    pub async fn write_item<I>(&mut self, item: &I) -> anyhow::Result<()>
    where
        I: Serialize + ?Sized,
    {
        if self.following_item {
            self.writer.write_all(b",\n").await?;
            self.buffer.clear();
        } else {
            self.following_item = true;
        }

        serde_json::to_writer_pretty(&mut self.buffer, item)?;

        self.writer.write_all(self.buffer.as_slice()).await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_array_writer() {
        let mut writer = Vec::with_capacity(100);
        let mut array_writer = ArrayWriter::new(&mut writer);

        array_writer
            .write_open_bracket()
            .await
            .expect("should not fail");

        array_writer
            .write_item("abc")
            .await
            .expect("should not fail");
        array_writer
            .write_item("123")
            .await
            .expect("should not fail");
        array_writer
            .write_item("last")
            .await
            .expect("should not fail");

        array_writer
            .write_close_bracket()
            .await
            .expect("should not fail");

        assert_eq!(
            String::from_utf8_lossy(&writer),
            "[\n\"abc\",\n\"123\",\n\"last\"\n]\n"
        )
    }
}
