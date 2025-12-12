void main() {

    BlockingQueue<Integer> queue = new BlockingQueue<>(2);

    Thread producer = new Thread(() -> {
        try {
            for (int i = 1; i <= 5; i++) {
                System.out.println("Producing " + i);
                queue.put(i);
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    });

    Thread consumer = new Thread(() -> {
        try {
            for (int i = 1; i <= 5; i++) {
                int value = queue.take();
                System.out.println("Consuming " + value);
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    });

    producer.start();
    consumer.start();
}
