# Kafka Partition Reassignment & Leader Election Plan Tool

This Python-based CLI tool simplifies the process of generating **partition reassignment** and **preferred leader election plans** for Apache Kafka. 

Designed for **Kafka Administrators** and **Technical Support Engineers**, it offers an interactive, safe, and guided workflow to generate a rebalance topic partitions and preferred leader election plan.

## Features
- Interactive CLI Workflow: Guided, step-by-step prompts for Kafka connection, topic filtering, broker selection, and plan export.
- Topic Filtering & Analysis: Fetches metadata and auto-classifies topics as internal or custom-defined.
- Partition Reassignment Plan Generator: Randomly redistributes partition replicas and exports Kafka-compatible JSON plans for balancing or broker decommissioning.

⭐ Preferred Leader Election Plan

Generates leader election plans targeting preferred brokers, compatible with Kafka CLI and Admin APIs.

📄 Next Steps & CLI Guidance

Prints ready-to-use Kafka commands for applying reassignment and leader election plans.

📝 Comprehensive Logging

Logs all actions, errors, and warnings with timestamps to kafka_script.log.

✅ Safe & Controlled Operations

Validates inputs, confirms actions, and exits gracefully on user cancellation or error.

🔄 Automatic Metadata Refresh

Refreshes topic and broker metadata on demand to reflect real-time cluster state.

## 📌 Why Use This Tool?

Managing partition placement and broker leadership in Kafka via manual JSON editing or shell scripts is time-consuming and error-prone. This tool provides:

- A **guided, repeatable workflow**
- Output fully compatible with Kafka CLI tools
- **Built-in safety checks** and validation
- Helpful for **broker decommission**, **cluster rebalancing**, and **support troubleshooting**

## 🛠️ Requirements

- **Python 3.6+**

- **Kafka CLI Tools** installed and available in your `PATH`. To use Kafka CLI tools like `kafka-reassign-partitions.sh` or `kafka-leader-election.sh`, ensure the Kafka `bin` directory is included in your system's `PATH`.
    > Setting Up Kafka CLI Tools in Your PATH (Linux/macOS)
    >  #### 🔄 For the Current Terminal Session
    > - ``` export PATH=/path/to/kafka/bin:$PATH ``` This change applies only to the current terminal session.
    > #### ♾️ To Make It Permanent (All Sessions)
    > Add the export line to your shell configuration file (e.g., ~/.bashrc or ~/.zshrc):
    > - ``` echo 'export PATH=/path/to/kafka/bin:$PATH' >> ~/.bashrc ```
    > - ``` source ~/.bashrc ``` (For zsh users, replace `~/.bashrc` with `~/.zshrc`.)
    > - 🔁 Note: Replace `/path/to/kafka/bin` with the actual path where Kafka binaries is installed. Example: `export PATH=/opt/confluent/bin:$PATH`
- **Kafka Cluster Access (with appropriate permissions)**
- **Client Configuration File:** If authentication is required (e.g., `SASL/SSL`), ensure you have the appropriate Kafka client config properties file.

## Usage

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

## Example Workflow

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

## Output Files
* kafka_script.log: Detailed session and operation logs.
* reassign-partitions-plan.json: Partition reassignment plan.
* leader-election-plan.json: Leader election plan (if applicable).

## Recommended Kafka CLI Usage
After plan generation, run commands like:
```bash
kafka-reassign-partitions --bootstrap-server <host:port> --command-config <client.properties> --reassignment-json-file reassign-partitions-plan.json --execute
kafka-reassign-partitions --bootstrap-server <host:port> --command-config <client.properties> --reassignment-json-file reassign-partitions-plan.json --verify
kafka-leader-election --bootstrap-server <host:port> --command-config <client.properties> --path-to-json-file leader-election-plan.json --election-type preferred
```
Follow the on-screen "NEXT STEPS" instructions output by the script.

## Notes
* The tool does not directly modify your Kafka cluster. It only generates JSON plans. Execution is delegated to the Kafka CLI.
* Plan generation ensures replica diversity and validates broker assignments for operational safety.
* Verify the output file before performing final execution.

## Troubleshooting
If topic metadata cannot be loaded, or if any command fails, errors will be displayed and saved to kafka_script.log. Review the log file for detailed diagnostics.
