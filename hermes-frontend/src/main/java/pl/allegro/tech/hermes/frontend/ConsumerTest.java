package pl.allegro.tech.hermes.frontend;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class ConsumerTest implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;

    public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
    }

    public void run() {
        try {
            ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
            while (it.hasNext()) {
                File file = new File("./messages");
                FileOutputStream fos = new FileOutputStream(file);
                fos.write(it.next().message());
            }
            it.next().message();
            System.out.println("Shutting down Thread: " + m_threadNumber);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
