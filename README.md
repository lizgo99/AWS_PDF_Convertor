# Distributed PDF Processing in AWS
by Liz Gokhvat [208005777] , Ido Toker [207942186]
## Overview
This project implements a distributed system to process PDF files in the cloud. Users provide an input file containing URLs of PDF files along with desired operations (e.g., `ToImage`, `ToText`, `ToHTML`). The system processes these files using a distributed architecture leveraging AWS services (S3, SQS, EC2) for scalability and reliability.

## Running the Program
To run the application:

First, build the project using Maven:
```bash
mvn clean package
```
Then, run the Local Application with the following command:
```bash
java -jar LocalApp.jar <inputFileName> <outputFileName> <n> [terminate|purge]
```
Where:
- `LocalApp.jar` is the JAR file for the Local Application
- `inputFileName`: Path to the input file containing operations and PDF URLs
- `outputFileName`: Path for the generated HTML output file
- `n`: Number of PDF files processed per worker
- `terminate`: (Optional) If provided, terminates the Manager instance after processing
- `purge`: (Optional) Alternative to terminate, cleans up resources

### Input File Format
Each line in the input file should follow this format:
```
<operation><\t><pdf_url>
```
Where `<operation>` and `<pdf_url>` are separated by a tab character.

Example:
```
ToImage	https://example.com/sample.pdf
ToText	https://example.com/document.pdf
ToHTML	https://example.com/report.pdf
```

- **Processing Time**: 
  1. One application took approximately 90 seconds for an input file of 100 PDF URLs with `n=10`.
  2. One application took approximately 110 seconds for an input file of 100 PDF URLs with `n=20`.
  3. One application took approximately 8 minutes for an input file of 2500 PDF URLs with `n=100`.
  4. Three applications, each with an input file of 100 PDF URLs with n=10 took approximately 2 minutes.
  5. Four applications, each with an input file of 2500 PDF URLs with n=100 took approximately 30 minutes.

## System Details
### Instance Details
- **AMI ID**: `ami-0fc8b0d8fe87f1e9b`
- **Instance Type**: `t2.micro`

## Architecture
### Components
1. **Local Application**:
    - Uploads input files to a dedicated S3 bucket with a unique identifier for each task
    - Creates and manages SQS queues for bi-directional communication with the Manager
    - Continuously polls SQS to monitor task completion and processing status
    - Aggregates results and generates a comprehensive HTML summary of processed PDFs
    - Implements termination and purge functionalities for complete resource cleanup
    - Handles error scenarios and provides appropriate user feedback

2. **Manager**:
    - Manages the entire processing workflow from a single EC2 instance:
      1. Receives tasks from Local Application via SQS
      2. Creates multiple threads to handle different responsibilities:
         - Task Distribution Thread: Continuously polls SQS for new tasks and distributes them to workers
         - Worker Management Thread: Monitors workload and dynamically scales EC2 worker instances
         - Result Collection Thread: Gathers results from workers using thread-safe ConcurrentLinkedQueue
      3. Utilizes TaskTracker with atomic counters for thread-safe task progress monitoring
      4. Implements worker health checks and manages instance lifecycle
      5. Sends completion notifications back to Local Application via SQS
    - Ensures efficient resource utilization and system reliability through concurrent operations

3. **Workers**:
    - Executes PDF conversion operations:
        - ToImage: Converts PDF pages to image files (e.g., PNG, JPEG)
        - ToText: Extracts plain text content from PDF
        - ToHTML: Transforms PDF into structured HTML format
    - Implements intelligent idle detection:
        - Automatically terminates after 60 seconds without new tasks
        - Periodically checks SQS queue for pending messages
    - Enhances task processing reliability:
        - Utilizes SQS visibility timeout to prevent task duplication
        - Implements exponential backoff for failed task retries
    - Streamlines result handling:
        - Uploads processed files directly to designated S3 buckets
        - Send completion notifications to Manager via SQS
        - Include metadata (e.g., processing time, file size) in notifications
 
### Flow Details
1. **Task Lifecycle**:
   - Local App initiates with unique ID, S3 setup, and Manager activation
   - Manager dissects input, optimizing distribution with `pdfsPerWorker`
   - Workers fetch tasks, process PDFs, and upload results to S3
   - Manager aggregates results, using `TaskTracker` for reliability
   - Summary generated and Local App notified upon completion

2. **Communication Flow**:
   - Local App → Manager: Task submission via `LocalAppToManager` queue
   - Manager → Workers: Task distribution through `ManagerToWorkers` queue
   - Workers → Manager: Completion notifications via `WorkersToManager` queue
   - Manager → Local App: Final notification using `ManagerToLocalApp` queue

## Key Features
### Security 
Instead of sending credentials in plain text, we're using AWS IAM roles to manage access securely. We've made sure not to include any plaintext credentials in our codebase. For S3 bucket access, we're using unique IDs for each task to ensure isolation. We've also implemented automatic resource cleanup to prevent any lingering assets that could pose a security risk.

### Scalability
Scalability was a major focus for us. We've designed the system to handle a large number of concurrent clients. While we haven't tested with millions of connections, our architecture should theoretically support it. Although there is a limit of 9 concurrent instances for students, we believe that our system can handle alot more than that thanks to the dynamic worker scaling based on queue depth that we implemented. This allows the system to adapt to varying loads.
We've also made sure to use thread-safe task tracking and result collection methods, including atomic counters and thread-safe queues, to ensure reliable operation under high load.
Also we are not downloading the whole PDF file in the worker, but we are using a stream to read it. So we have to only read and process a small part of the file (the first page).
Even if we will get a large PDF file, the system will still be able to handle it.

### Persistence
We've considered various failure scenarios in our design. If a node dies or stalls, our system can recover. We're using SQS visibility timeout management to handle cases where a worker might fail to process a message. We also have worker health monitoring in place. For resource management, we've implemented automatic worker termination after periods of inactivity, and we make sure to clean up queues and buckets. We've also built in a graceful system shutdown process.

### Threads
We've used threads in our application to enable concurrent processing, which is crucial for performance in a distributed system like ours. However, we've been careful to manage these threads properly to avoid contention issues. We've tested running multiple clients simultaneously and made sure they all work correctly, finish properly, and produce accurate results.
For termination, we've implemented a process to ensure all resources are properly closed and cleaned up when requested.

### System Limitations
We're aware of the EC2 instance limits (9 concurrent instances for students) and have designed our system to work within these constraints. We're also keeping an eye on other AWS service quotas.

### Performance Optimization
We've tried to ensure that our workers are working efficiently. They're designed to maximize CPU usage when processing tasks. For the manager, we've been careful to limit its role to orchestration to avoid it becoming a bottleneck. We've clearly defined the tasks for each component of the system to maintain separation of concerns.

To optimize performance further, we've implemented a few key features:
1. Worker Timeout: Workers automatically terminate after 60 seconds of inactivity to free up resources.
2. Message Visibility: We've set the SQS message visibility timeout to 60 seconds for task processing to prevent duplicate work.
3. Concurrent Processing: Workers process tasks in parallel to maximize throughput.

Regarding the distributed nature of our system, we've made sure that components aren't unnecessarily waiting on each other. The asynchronous nature of our message queues helps ensure this, allowing each part of the system to work independently as much as possible.

## Development
### Build Requirements
- Java 8 or higher
- Maven for dependency management
- AWS SDK v2.x
- Valid AWS credentials/configuration

### Project Structure
```
PdfConverter/
├── LocalApp/      # Local application component
├── Manager/       # Manager service component
└── Worker/        # Worker service component
```

Each component is a separate Maven project with its own dependencies and build configuration.
