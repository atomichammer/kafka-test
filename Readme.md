Kafka is set up using this [tutorial](https://habr.com/ru/post/543732/)

- Producer reads input folder every 10 seonds and takes a \*.csv file that is older than 60 seconds. Strips line endings and streams it line-by-line to kafka topic **demo-topic**. After the last line it puts an EOF marker message.
- Consumer reads all messages until a EOF marker and writes them down to a file named in timestamp **ddmmyyyyhh24miss.csv**
- Need to add timeout on waiting for eof.
- Maybe need to pass metainformation about the file as SOF message so that consumer knew what to expect.

