# Kafka Partition Reassignment & Leader Election Plan Tool

This Python utility simplifies the process of generating Kafka partition reassignment plans and preferred leader election plans. Designed for administrators, it provides a guided interactive experience to rebalance topics and promote preferred brokers, with robust logging and validation for safe, controlled operations.

---
## ## Features

* **Interactive CLI Workflow**: Prompts users to input connection settings, filters, and confirmations throughout all steps.
* **Topic Filtering & Analysis**: Connects to a Kafka cluster, describes topics/partitions, and classifies topics as custom or internal.
* **Partition Reassignment Plan Generator**: Randomly redistributes replicas among available brokers, exporting plans in Kafka compatible JSON.
* **Preferred Leader Election Plan**: Lets you select a broker as preferred leader for filtered topics/partitions and generates required plan files.
* **Next Steps & CLI Guidance**: Prints explicit command lines and guidance to use with standard Kafka tools (`kafka-reassign-partitions`, `kafka-leader-election`, etc.).
* **Comprehensive Logging**: All actions, warnings, and errors are logged to `kafka_script.log` with timestamps.
* **User Safety**: Extensive input validation, confirmation prompts, and graceful exit handling on cancellation or errors.
* **Automatic Data Refresh**: Refresh Kafka topic metadata and continue workflow as needed.

---
## ## Requirements

* Python 3.6+
* Kafka CLI Tools installed and on your `PATH` (`kafka-topics`, `kafka-reassign-partitions`, `kafka-leader-election`)
* Kafka Cluster Access (with appropriate permissions)
* Client Config File (e.g., SASL/SSL properties if required for authentication)

---
## ## Usage

1.  Launch the script from your terminal:
    ```bash
    python3 kafka_partition_reassignment_tool.py
    ```

2.  Upon launch, you will be prompted to:
    * **Provide Kafka Bootstrap Server**: (example: `kafka.example.com:9092`)
    * **Specify Security (SASL/SSL)**: If enabled, input the path to your client properties file.
    * **Define a Topic Filter**: Use a substring, regex, or `*` for all topics.

3.  After initial metadata is loaded, the CLI menu offers these actions:

    * **Build a reassign partitions plan**: Randomizes replica distribution, ensuring replica sets are diversified across brokers.
    * **Build a preferred leader election plan**: Lets you designate a broker as leader for selected/filtered topics and partitions.
    * **Exit**: Cleanly ends the session.

Each plan is exported as JSON files (e.g., `reassign-partitions-plan.json`, `leader-election-plan.json`) ready for use with the Kafka CLI tools.

---
## ## Example Workflow

```text
┌─────────────────────────────────────────────────────────────┐
│  Kafka Partition Reassignment & Leader Election Plan Tool   │
└─────────────────────────────────────────────────────────────┘
------------------------------------------------------------------------------------------
Connection Setup and Initial Data Load
------------------------------------------------------------------------------------------
Enter the Kafka bootstrap server (host:port) (e.g., kafka.example.com:9092):
> kafka1:9092

Is security enabled (SASL/SSL)? [y/n]:
> y

Enter full path to client properties file (e.g., /etc/kafka/client.properties):
> /home/user/client.properties

Enter a topic name filter for initial load (substring, regex, or * for all):
> payments

Total Topics Loaded: 2
Total Partitions in Scope: 10

Select an operation:
  1. Build a reassign partitions plan.
  2. Build a preferred leader election plan.
  3. Exit

Choose [1/2/3]:
> 1

Action: Build a reassign partitions plan
[... outputs summary, asks for confirmation, writes plan file, shows next steps ...]
```
You will be prompted for inputs and confirmation before any changes, with clear instructions on what to do next.

Output Files
kafka_script.log: Session history and detailed logs
reassign-partitions-plan.json: Partition reassignment plan for use with Kafka CLI tools
leader-election-plan.json: Leader election plan file (if generated)

Output Files
kafka_script.log: Detailed session and operation logs.
reassign-partitions-plan.json: Partition reassignment plan.
leader-election-plan.json: Leader election plan (if applicable).

Recommended Kafka CLI Usage
After plan generation, run commands like:

kafka-reassign-partitions --bootstrap-server <host:port> --command-config <client.properties> --reassignment-json-file reassign-partitions-plan.json --execute
kafka-reassign-partitions --bootstrap-server <host:port> --command-config <client.properties> --reassignment-json-file reassign-partitions-plan.json --verify
kafka-leader-election --bootstrap-server <host:port> --command-config <client.properties> --path-to-json-file leader-election-plan.json --election-type preferred
bash

Follow the on-screen "NEXT STEPS" instructions output by the script.

Notes
The tool does not directly modify your Kafka cluster. It only generates JSON plans. Execution is delegated to the Kafka CLI.
All interactive prompts are safeguarded against accidental exit or invalid input.
Plan generation ensures replica diversity and validates broker assignments for operational safety.
Troubleshooting
If topic metadata cannot be loaded, or if any command fails, errors will be displayed and saved to kafka_script.log. Review the log file for detailed diagnostics.
