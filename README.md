# Spark lab: from movie ratings to live trends

A 90-minute practical for the Data Stream course. We start with historical
MovieLens ratings, explore them with Spark DataFrames, train an ALS recommender,
and watch synthetic movie plays arrive through Structured Streaming.

Everything runs on your laptop with **two Spark worker threads** (`local[2]`).
No external services are needed. The data are included; after setup, the lab
works offline.

## Before the practical

Complete installation and the environment check **before class**. Allow time
and several GB of free disk space for downloads. Miniforge supplies Python,
Java and the environment manager; you do not need to install them separately.
Only the Miniforge setup below is supported for this course.

### 1. Install Miniforge for your operating system

Use the [official Miniforge installers](https://github.com/conda-forge/miniforge#install).

**Windows 11 (x86-64)**

1. Download and run `Miniforge3-Windows-x86_64.exe` from the official page.
2. Install for your user and enable Start menu shortcuts. Prefer an installation
   directory without spaces or accented characters. Keep the default PATH option.
3. Open **Miniforge Prompt** from the Start menu for every command below.
   These instructions do not require PowerShell configuration.

**macOS**

1. Download `Miniforge3-MacOSX-arm64.sh` for Apple Silicon, or
   `Miniforge3-MacOSX-x86_64.sh` for an Intel Mac.
2. In Terminal, go to the download directory and run the matching command:

   ```bash
   bash Miniforge3-MacOSX-arm64.sh
   ```

   For Intel, replace `arm64` with `x86_64`. Accept shell initialization when asked.
3. Close and reopen Terminal.

**Linux (x86-64)**

1. Download `Miniforge3-Linux-x86_64.sh`.
2. In a terminal in the download directory, run:

   ```bash
   bash Miniforge3-Linux-x86_64.sh
   ```

3. Accept shell initialization, then close and reopen the terminal.

### 2. Get the lab and create the environment

Download and extract this repository's ZIP (Git is optional), or clone it.
In your terminal, `cd` to the extracted directory containing `environment.yml`:

```bash
conda env create -f environment.yml
conda activate data-stream-spark
python check_environment.py
```

Wait for **Environment ready.** The check tests Python, Java, a DataFrame
operation, ALS fitting/prediction, and a file-based streaming aggregation.
It usually takes longer on its first run as the JVM starts.

The environment selects **Python 3.11, Java 17 and PySpark 4.0.1–4.0.x**.
The checker and notebook select the environment's Java and Python
automatically. No manual `JAVA_HOME`, `SPARK_HOME`, or `PATH` setup is needed.

## Starting the lab

From the repository directory:

```bash
conda activate data-stream-spark
jupyter lab
```

Open **`lab.ipynb`** and choose **Python 3 (ipykernel)**. Run the cells in order.
**Given** cells provide boilerplate. **TODO** cells are executable starting
points: their provisional values and missing analyses are intentional; follow
the nearby instructions to complete them. Running every starter cell is safe,
but does not mean you have answered every exercise.

Use one notebook kernel at a time to keep laptop memory usage modest.

| Topic | Minutes |
| --- | ---: |
| Environment check | 5 |
| DataFrames | 15 |
| Spark and iterative ML | 5 |
| ALS recommender | 25 |
| From batch to stream | 5 |
| Structured Streaming | 25 |
| Buffer, discussion, optional watermark | 10 |

Optional material does not feed any later exercise.

## Streaming section

The notebook first publishes a small replay, so **Run All** also works without
a second terminal. For live activity, leave Jupyter open and start this in a
**second terminal**, also in the repository directory:

```bash
conda activate data-stream-spark
python streaming/generate_events.py
```

Keep it running while you work through the streaming exercises. It creates a
complete JSON file every two seconds in `streaming/runtime/events/`, with 40
plays across 20 movies. After 20 seconds, a movie gets a 30-second popularity
burst; the cycle repeats with different movies.

Press **Ctrl+C in the producer terminal** to stop it. The notebook's final
cleanup cell stops all queries and Spark. To stop Jupyter, shut down the kernel
and press Ctrl+C in its terminal.

Rerunning a query replays the existing input files. For a fresh start, first
stop the producer and the notebook kernel, then delete only
`streaming/runtime/` using your file manager. The next run recreates it.

## Troubleshooting

- **Environment not activated / wrong Python:** run `conda activate
  data-stream-spark`, then `python check_environment.py` in the same terminal.
- **Wrong Java:** use the lab checker/setup cell; they select Conda's Java 17.
  If it is missing, run `conda env update -f environment.yml`, reactivate the
  environment and retry. Do not install a separate Spark distribution.
- **Jupyter uses another environment:** stop Jupyter, activate this environment
  and launch `jupyter lab` again. The setup cell prints its interpreter path.
- **Windows commands fail:** use **Miniforge Prompt**, not an unconfigured
  PowerShell. If the checker log names `winutils` or `NativeIO`, send the log
  to your instructor.
- **No changing trends:** check that the producer is still writing files, then
  rerun the trends display cell. Without the producer, you see only a replay.

Failed checks point to `validation-results/environment-spark.log`, which keeps
the detailed JVM diagnostics out of the checklist. Send it to your instructor
if you need help.

## Data

MovieLens contains 100,836 ratings from 610 users. Its original terms and
attribution accompany the data in [`data/`](data/README.md).
The event producer makes synthetic plays; ratings are not viewing histories.
