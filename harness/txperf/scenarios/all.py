from txperf.scenarios.redpanda_money_scenario import RedpandaMoneyScenario
from txperf.scenarios.redpanda_subscribe_scenario import RedpandaSubscribeScenario
from txperf.scenarios.kafka_money_scenario import KafkaMoneyScenario
from txperf.scenarios.kafka_subscribe_scenario import KafkaSubscribeScenario

SCENARIOS = {
    "redpanda_money_scenario": RedpandaMoneyScenario,
    "redpanda_subscribe_scenario": RedpandaSubscribeScenario,
    "kafka_money_scenario": KafkaMoneyScenario,
    "kafka_subscribe_scenario": KafkaSubscribeScenario
}