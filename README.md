# message-transmitter

A library for simple work with message flows between brokers. Allows you to configure multiple sources and consumers and describe the processing of records in a functional-declarative style.
Connects as a starter to your spring boot project

Use case:

    @Autowired
    private Transporter<Person> transporter;
    @Autowired
    private PersonRepository repository;

    @EventListener(ContextRefreshedEvent.class)
    public void work(ContextRefreshedEvent event){
        transporter
                .mappingExHandler(ex -> System.out.println(ex.getMessage()))
                .pipeline()
                .filter(person -> person.getAge() > 18)
                .forEachAndThen(repository::save)
                .map(Unit::fromPerson)
                .forEachAndThen(System.out::println)
                .sendTo("test_deal");

application.yaml:

    transmitter:
        enabled: true
        source-topic: test_loan
        broker:
            consumer:
                bootstrap-servers: localhost:9092
            ssl:
                protocol: PLAINTEXT
                group-id: test-group
            producer:
                bootstrap-servers: localhost:9093
            ssl:
                protocol: SSL
        routes:
          - source-topic: test
            name: testing
            consumer:
                bootstrap-servers: localhost:9092
                ssl:
                    protocol: PLAINTEXT
                group-id: test-group
            producer:
                bootstrap-servers: localhost:9092
                ssl:
                protocol: PLAINTEXT

    
