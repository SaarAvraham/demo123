package com.example.demo;

import com.google.gson.stream.JsonWriter;
import lombok.SneakyThrows;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@RestController
@SpringBootApplication
public class DemoApplication {
    public static void main(String[] args) throws IOException {
        SpringApplication.run(DemoApplication.class, args);
//        testFluxToStream();
//        testServerLess();
    }

    private void justBlockingQueueFixed(OutputStream out) throws IOException {
        BlockingQueue<String> blockingQueue = new ArrayBlockingQueue<>(40000);
        AtomicBoolean isDone = new AtomicBoolean(false);
        Instant start = Instant.now();
        Stream<String> stringStream = IntStream.range(0, 5000)
                .boxed()
                .map(i -> i + " fjdjkdsnkfsdfs");
        JsonWriter writer = new JsonWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
        writer.setIndent("  ");
        writer.beginArray();

        Thread thread = new Thread(() -> { // stream from DB
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            stringStream.forEach(s -> {
                try {
                    blockingQueue.put(s);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });

            isDone.set(true);
        });
        thread.start();

        try {
            while (!isDone.get() || !blockingQueue.isEmpty()) {
                try {
                    List<String> batch = new ArrayList<>();
                    String s = blockingQueue.poll(1, TimeUnit.SECONDS);
                    if (s == null) {
                        writeListAsArray(writer, new ArrayList<>());
                    } else {
                        batch.add(s);
                        blockingQueue.drainTo(batch);
                        writeListAsArray(writer, batch);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            System.out.println(Thread.currentThread());
            System.out.println(Duration.between(start, Instant.now()));
            System.out.println("Closing array!!!!!!!!!!!");
            writer.endArray();

            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void justBlockingQueue(OutputStream out) throws IOException {
        BlockingQueue<String> blockingQueue = new ArrayBlockingQueue<>(40000);
        AtomicBoolean isDone = new AtomicBoolean(false);
        Instant start = Instant.now();
        Stream<String> stringStream = IntStream.range(0, 5000)
                .boxed()
                .map(i -> i + " fjdjkdsnkfsdfs");
        JsonWriter writer = new JsonWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
        writer.setIndent("  ");
        writer.beginArray();

        Thread thread = new Thread(() -> {
            try {
                while (!isDone.get() || !blockingQueue.isEmpty()) {
                    try {
                        List<String> batch = new ArrayList<>();
                        String s = blockingQueue.take();
                        batch.add(s);
                        blockingQueue.drainTo(batch);
                        writeListAsArray(writer, batch);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                System.out.println(Thread.currentThread());
                System.out.println(Duration.between(start, Instant.now()));
                System.out.println("Closing array!!!!!!!!!!!");
                writer.endArray();

                writer.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        thread.start();


        stringStream.forEach(s -> {
            try {
                blockingQueue.put(s);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        isDone.set(true);
    }

    private void justOnAnotherThreadThisIsBuggyShowTal(OutputStream out) throws IOException {
        Thread thread = new Thread(() -> {
            try {
                Instant start = Instant.now();
                Stream<String> stringStream = IntStream.range(0, 5000000)
                        .boxed()
                        .map(i -> i + " fjdjkdsnkfsdfs");
                JsonWriter writer = new JsonWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
                writer.setIndent("  ");
                writer.beginArray();
                List<String> batch = new ArrayList<>();
                Iterator<String> iterator = stringStream.iterator();

                while (iterator.hasNext()) {
                    batch.add(iterator.next());
                    if (batch.size() == 800) {
                        writeListAsArray(writer, batch);
                        batch = new ArrayList<>();
                    }
                }

                if (batch.size() > 0) {
                    writeListAsArray(writer, batch);
                }

                System.out.println(Thread.currentThread());
                System.out.println(Duration.between(start, Instant.now()));
                System.out.println("Closing array!!!!!!!!!!!");
                writer.endArray();
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        thread.start();
    }

    ExecutorService executor = Executors.newSingleThreadExecutor();

    @GetMapping("/gson")
    private StreamingResponseBody getAllEmployees() {
        return out -> justBlockingQueueFixed(out);
//        return out -> justBlockingQueue(out);
//        return out -> justOnAnotherThreadThisIsBuggyShowTal(out);
//        return out -> { // TODO: 2/27/2021 Saar - try to write to outputstream with original thread (parent)
//            BlockingQueue<Batch> blockingQueue = new ArrayBlockingQueue<>(4);
//            Instant start = Instant.now();
//            JsonWriter writer = new JsonWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
//            AtomicReference<JsonWriter> jsonWriterAtomicReference = new AtomicReference<>(writer);
//            writer.setIndent("  ");
//            writer.beginArray();
//            Stream<String> stringStream = IntStream.range(0, 5000000)
//                    .boxed()
//                    .map(i -> i + " fjdjkdsnkfsdfs");
//            AtomicBoolean isDone = new AtomicBoolean(false);
//
//            System.out.println(Thread.currentThread().getName());
////            Flux<List<String>> using = Flux.using(() -> writer,
////                    writer1 -> Flux.fromStream(stringStream)
//////                            .publishOn(Schedulers.newSingle("Saar1"))
//////                            .delayElements(Duration.ofMillis(2))
////                            .bufferTimeout(80000, Duration.ofMillis(10))
//////                            .buffer(800)
//////                            .publishOn(Schedulers.newSingle("Saar2"))
////                            ,
////                    writer1 -> {
////                        System.out.println("In cleaning lambda! " + Thread.currentThread());
////                        System.out.println(Duration.between(start, Instant.now()).getSeconds());
////                        System.out.println("Closing array!!!!!!!!!!!");
////                        try {
////                            writer.endArray();
////                        } catch (IOException e) {
////                            System.out.println("Failed to end array");
////                            e.printStackTrace();
////                        }
////                        try {
////                            writer.close();
////                        } catch (IOException e) {
////                        }
////                    });
////            using.doOnError(Throwable::printStackTrace)
////                    .subscribe(s -> writeListAsArray(jsonWriterAtomicReference, s));
////                    .subscribe(s -> {
////                        Future<?> submit = executor.submit(() -> writeListAsArray(writer, s));
////                        try {
////                            submit.get();
////                        } catch (Exception e) {
////                            System.out.println("FAILED IN GET FUTURE");
////                            e.printStackTrace();
////                        }
////                    });
//            Mono.fromSupplier(() -> "first string")
//                    .delayElement(Duration.ofSeconds(5))
//                    .concatWith(Flux.fromStream(stringStream))
////            Flux.fromStream(stringStream)
////                    .delayElements(Duration.ofSeconds(3))
//                    .windowTimeout(20000, Duration.ofSeconds(2))
////                    .bufferTimeout(20000, Duration.ofMillis(200))
////                    .window(Duration.ofMillis(500))
////                    .subscribe(s -> executor.submit(() -> writeListAsArray(writer, s)));
//                    .publishOn(Schedulers.boundedElastic())
//                    .subscribe(strings -> {
//                                try {
//                                    blockingQueue.put(new Batch(strings.collectList().block()));
//                                } catch (InterruptedException e) {
//                                    e.printStackTrace();
//                                }
//                            }
////                            strings.forEach(s -> {
////                        try {
////                            blockingQueue.put(s);
////                        } catch (InterruptedException e) {
////                            System.out.println("Error was in" + Thread.currentThread().getName());
////                            e.printStackTrace();
////                        }
////                    })
//                    , Throwable::printStackTrace, () -> isDone.set(true));
//
////            stringStream.forEach(s -> {
////                try {
////                    writer.value(s);
////                } catch (IOException e) {
////                    e.printStackTrace();
////                }
////            });
////            Thread thread = new Thread(() -> {
//            while (!isDone.get() || !blockingQueue.isEmpty()) {
//                try {
//                    Batch b = blockingQueue.take();
//                    List<String> batch = b.getBatch();
//                    writeListAsArray(writer, batch);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//
//            System.out.println(Duration.between(start, Instant.now()).getSeconds());
//            System.out.println("Closing array!!!!!!!!!!! " + Thread.currentThread().getName());
//            try {
//                writer.endArray();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            try {
//                writer.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
////            });
////            thread.start();
//        };
    }

    private void writeListAsArray(AtomicReference<JsonWriter> jsonWriterAtomicReference, List<String> strings) {
        System.out.println("Stating to write list of " + strings.size() + " elements, " + Thread.currentThread().getName());
        jsonWriterAtomicReference.updateAndGet(writer -> {
            try {
                writer.beginArray();
            } catch (IOException e) {
                e.printStackTrace();
            }
            for (String s : strings) {
                try {
                    writer.value(s);
//                System.out.println(Thread.currentThread()+ " wrote value " + s);
                } catch (IOException e) {
                    System.out.println("ERROR while writing value! " + Thread.currentThread());
                    e.printStackTrace();
                }
            }

            try {
                writer.endArray();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return writer;
        });
    }

    private static void testServerLess() throws IOException {
        Instant start = Instant.now();
        JsonWriter writer = new JsonWriter(new OutputStreamWriter(new OutputStream() {

            @Override
            public void write(int b) throws IOException {
                synchronized (this) {

                }
            }
        }, StandardCharsets.UTF_8));
        writer.setIndent("  ");
        writer.beginArray(); //TODO: 2/26/2021 might be outofmemeory problem
        Stream<String> stringStream = IntStream.range(0, 50000)
                .boxed()
                .map(i -> i + " fjdjkdsnkfsdfs");
//// TODO: 2/26/2021 bufferTimeout probably uses thread pool, so the main thread closes the writer before the subscriber ends.
//        Flux.fromStream(stringStream)
////                    .buffer(5)
//                .bufferTimeout(500000000, Duration.ofSeconds(1))
////                    .window(Duration.ofMillis(500))
////                    .subscribe(s -> executor.submit(() -> writeListAsArray(writer, s)));
//                .publishOn(Schedulers.immediate())
//                .subscribe(s -> writeListAsArray(writer, s));
//
//        System.out.println(Thread.currentThread());
//        System.out.println(Duration.between(start, Instant.now()).getSeconds());
//        System.out.println("Closing array!!!!!!!!!!!");
//        writer.endArray();
//        writer.close();

        Flux<List<String>> using = Flux.using(() -> writer,
                writer1 -> Flux.fromStream(stringStream)
//                        .delayElements(Duration.ofMillis(2))
                        .bufferTimeout(80000, Duration.ofMillis(1))
//                .buffer(1000)
                        .publishOn(Schedulers.single())
                ,
                writer1 -> {
                    System.out.println(Thread.currentThread());
                    System.out.println(Duration.between(start, Instant.now()).getSeconds());
                    System.out.println("Closing array!!!!!!!!!!!");
                    try {
                        writer.endArray();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    try {
                        writer.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
        using.subscribe(s -> writeListAsArray(writer, s));
    }

    @SneakyThrows
    private static void writeListAsArray(JsonWriter writer, List<String> strings) {
        synchronized (writer) {
            System.out.println("Stating to write list of " + strings.size() + " elements, " + Thread.currentThread());

            writer.beginArray();
            for (String s : strings) {
                try {
                    writer.value(s);
//                System.out.println(Thread.currentThread()+ " wrote value " + s);
                } catch (IOException e) {
                    System.out.println("ERROR while writing value! " + Thread.currentThread());
                    ;
                    e.printStackTrace();
                    throw e;
                }
            }

            writer.endArray();
            writer.flush();
        }
    }

    @SneakyThrows
    private synchronized void writeListAsArray(JsonWriter writer, Flux<String> strings) {
        System.out.println("Stating to write array " + Thread.currentThread());

        writer.beginArray();
        strings.subscribe(s -> {
            try {
                writer.value(s);
//                System.out.println(Thread.currentThread()+ " wrote value " + s);
            } catch (IOException e) {
                System.out.println("ERROR: " + Thread.currentThread());
                e.printStackTrace();
            }
        });

        writer.endArray();
    }
}
