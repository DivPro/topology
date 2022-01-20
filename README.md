# topology
vizualize kafka topology from ksql+connect rest api with dot notation and graphviz

## Steps:
- first, add your program config file: copy cfg/example.yml to cfg/config.yml and set actual params for KSQL API and Kafka-Connect API
- then compile program (repo root - main.go) and you will see tho files in a program fs root: graph.png and graph.svg
- program output will contain queries and a fragment of code in a .dot notation
