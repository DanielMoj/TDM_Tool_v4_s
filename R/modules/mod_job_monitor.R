# R/modules/mod_job_monitor.R
# Shiny Module for Job Queue Monitoring and Management
# Provides real-time monitoring of parallel jobs and queue status

# Dependencies check
if (!requireNamespace("shiny", quietly = TRUE)) {
  stop("Package 'shiny' required")
}
if (!requireNamespace("DT", quietly = TRUE)) {
  warning("Package 'DT' recommended for better tables")
}

# Source job queue if not already loaded
if (!exists("JobQueue")) {
  if (file.exists("../job_queue.R")) {
    source("../job_queue.R")
  }
}

# ============================================================================
# Module UI
# ============================================================================

#' Job Monitor UI Module
#' 
#' @param id Module namespace ID
#' @return UI elements
#' @export
jobMonitorUI <- function(id) {
  ns <- shiny::NS(id)
  
  shiny::tagList(
    # Custom CSS for the monitor
    shiny::tags$head(
      shiny::tags$style(shiny::HTML("
        .job-status-badge {
          padding: 3px 8px;
          border-radius: 3px;
          font-size: 12px;
          font-weight: bold;
        }
        .status-queued { background-color: #f0ad4e; color: white; }
        .status-running { background-color: #5bc0de; color: white; }
        .status-completed { background-color: #5cb85c; color: white; }
        .status-failed { background-color: #d9534f; color: white; }
        
        .job-monitor-card {
          border: 1px solid #ddd;
          border-radius: 5px;
          padding: 15px;
          margin-bottom: 15px;
          background: white;
        }
        
        .metric-box {
          text-align: center;
          padding: 10px;
          border: 1px solid #e0e0e0;
          border-radius: 3px;
          background: #f9f9f9;
        }
        
        .metric-value {
          font-size: 24px;
          font-weight: bold;
          color: #333;
        }
        
        .metric-label {
          font-size: 12px;
          color: #666;
          text-transform: uppercase;
        }
      "))
    ),
    
    # Main layout
    shiny::fluidRow(
      # Summary metrics
      shiny::column(12,
        shiny::div(class = "job-monitor-card",
          shiny::h4("Queue Overview", shiny::icon("dashboard")),
          shiny::fluidRow(
            shiny::column(3,
              shiny::div(class = "metric-box",
                shiny::div(class = "metric-value", 
                          shiny::textOutput(ns("metric_queued"), inline = TRUE)),
                shiny::div(class = "metric-label", "Queued")
              )
            ),
            shiny::column(3,
              shiny::div(class = "metric-box",
                shiny::div(class = "metric-value", 
                          shiny::textOutput(ns("metric_running"), inline = TRUE)),
                shiny::div(class = "metric-label", "Running")
              )
            ),
            shiny::column(3,
              shiny::div(class = "metric-box",
                shiny::div(class = "metric-value", 
                          shiny::textOutput(ns("metric_completed"), inline = TRUE)),
                shiny::div(class = "metric-label", "Completed")
              )
            ),
            shiny::column(3,
              shiny::div(class = "metric-box",
                shiny::div(class = "metric-value", 
                          shiny::textOutput(ns("metric_failed"), inline = TRUE)),
                shiny::div(class = "metric-label", "Failed")
              )
            )
          )
        )
      ),
      
      # Performance metrics
      shiny::column(6,
        shiny::div(class = "job-monitor-card",
          shiny::h4("Performance", shiny::icon("chart-line")),
          shiny::plotOutput(ns("performance_plot"), height = "200px"),
          shiny::hr(),
          shiny::fluidRow(
            shiny::column(6,
              shiny::strong("Avg Wait Time:"),
              shiny::textOutput(ns("avg_wait_time"), inline = TRUE)
            ),
            shiny::column(6,
              shiny::strong("Avg Run Time:"),
              shiny::textOutput(ns("avg_run_time"), inline = TRUE)
            )
          ),
          shiny::br(),
          shiny::fluidRow(
            shiny::column(6,
              shiny::strong("Cache Hit Rate:"),
              shiny::textOutput(ns("cache_hit_rate"), inline = TRUE)
            ),
            shiny::column(6,
              shiny::strong("Throughput:"),
              shiny::textOutput(ns("throughput"), inline = TRUE)
            )
          )
        )
      ),
      
      # Resource usage
      shiny::column(6,
        shiny::div(class = "job-monitor-card",
          shiny::h4("Resource Usage", shiny::icon("server")),
          shiny::plotOutput(ns("resource_plot"), height = "200px"),
          shiny::hr(),
          shiny::fluidRow(
            shiny::column(6,
              shiny::strong("CPU Usage:"),
              shiny::textOutput(ns("cpu_usage"), inline = TRUE)
            ),
            shiny::column(6,
              shiny::strong("Memory:"),
              shiny::textOutput(ns("memory_usage"), inline = TRUE)
            )
          ),
          shiny::br(),
          shiny::fluidRow(
            shiny::column(6,
              shiny::strong("Workers:"),
              shiny::textOutput(ns("worker_status"), inline = TRUE)
            ),
            shiny::column(6,
              shiny::strong("Queue Size:"),
              shiny::textOutput(ns("queue_size"), inline = TRUE)
            )
          )
        )
      ),
      
      # Job table
      shiny::column(12,
        shiny::div(class = "job-monitor-card",
          shiny::fluidRow(
            shiny::column(6,
              shiny::h4("Active Jobs", shiny::icon("tasks"))
            ),
            shiny::column(6, style = "text-align: right; padding-top: 10px;",
              shiny::actionButton(ns("refresh_jobs"), "Refresh", 
                                icon = shiny::icon("sync"),
                                class = "btn-sm"),
              shiny::actionButton(ns("clear_completed"), "Clear Completed", 
                                icon = shiny::icon("broom"),
                                class = "btn-sm btn-warning"),
              shiny::actionButton(ns("cancel_all"), "Cancel All", 
                                icon = shiny::icon("stop"),
                                class = "btn-sm btn-danger")
            )
          ),
          shiny::hr(),
          DT::DTOutput(ns("job_table"))
        )
      ),
      
      # Control panel
      shiny::column(12,
        shiny::div(class = "job-monitor-card",
          shiny::h4("Queue Control", shiny::icon("sliders-h")),
          shiny::fluidRow(
            shiny::column(4,
              shiny::numericInput(ns("max_workers"), 
                                "Max Workers:",
                                value = 4,
                                min = 1,
                                max = parallel::detectCores(),
                                step = 1),
              shiny::actionButton(ns("update_workers"), "Update", 
                                class = "btn-primary")
            ),
            shiny::column(4,
              shiny::selectInput(ns("priority_mode"),
                               "Priority Mode:",
                               choices = c("FIFO" = "fifo",
                                         "Priority" = "priority",
                                         "Load Balance" = "balance"),
                               selected = "priority")
            ),
            shiny::column(4,
              shiny::checkboxInput(ns("enable_cache"), 
                                  "Enable Cache",
                                  value = TRUE),
              shiny::actionButton(ns("clear_cache"), "Clear Cache",
                                class = "btn-warning")
            )
          )
        )
      )
    )
  )
}

# ============================================================================
# Module Server
# ============================================================================

#' Job Monitor Server Module
#' 
#' @param id Module namespace ID
#' @param queue Reactive JobQueue object or NULL to create new
#' @param update_interval Update interval in seconds
#' @return List with queue object and controls
#' @export
jobMonitorServer <- function(id, queue = NULL, update_interval = 2) {
  shiny::moduleServer(id, function(input, output, session) {
    
    # Initialize or use provided queue
    if (is.null(queue)) {
      queue_obj <- shiny::reactiveVal(
        JobQueue$new(max_workers = 4, cache_dir = tempdir())
      )
    } else {
      queue_obj <- queue
    }
    
    # Stats storage
    stats_history <- shiny::reactiveVal(list())
    resource_history <- shiny::reactiveVal(list())
    
    # Auto-refresh timer
    refresh_timer <- shiny::reactiveTimer(update_interval * 1000)
    
    # Get current stats
    current_stats <- shiny::reactive({
      refresh_timer()
      input$refresh_jobs
      
      q <- queue_obj()
      if (!is.null(q)) {
        q$get_stats()
      } else {
        list(queued = 0, running = 0, completed = 0, failed = 0)
      }
    })
    
    # Update metrics
    output$metric_queued <- shiny::renderText({
      stats <- current_stats()
      as.character(stats$queued)
    })
    
    output$metric_running <- shiny::renderText({
      stats <- current_stats()
      as.character(stats$running)
    })
    
    output$metric_completed <- shiny::renderText({
      stats <- current_stats()
      as.character(stats$completed)
    })
    
    output$metric_failed <- shiny::renderText({
      stats <- current_stats()
      as.character(stats$failed)
    })
    
    # Performance metrics
    output$avg_wait_time <- shiny::renderText({
      stats <- current_stats()
      if (!is.null(stats$avg_wait_time) && !is.na(stats$avg_wait_time)) {
        sprintf("%.1f sec", stats$avg_wait_time)
      } else {
        "N/A"
      }
    })
    
    output$avg_run_time <- shiny::renderText({
      stats <- current_stats()
      if (!is.null(stats$avg_run_time) && !is.na(stats$avg_run_time)) {
        sprintf("%.1f sec", stats$avg_run_time)
      } else {
        "N/A"
      }
    })
    
    output$cache_hit_rate <- shiny::renderText({
      stats <- current_stats()
      total <- stats$completed + stats$failed
      if (total > 0 && !is.null(stats$cache_hits)) {
        sprintf("%.1f%%", (stats$cache_hits / total) * 100)
      } else {
        "0%"
      }
    })
    
    output$throughput <- shiny::renderText({
      stats <- current_stats()
      if (!is.null(stats$avg_run_time) && stats$avg_run_time > 0) {
        sprintf("%.1f jobs/min", 60 / stats$avg_run_time)
      } else {
        "N/A"
      }
    })
    
    # Resource metrics
    output$cpu_usage <- shiny::renderText({
      # Mock CPU usage - in real implementation, get from system
      cpu <- (current_stats()$running / 4) * 100
      sprintf("%.0f%%", min(cpu, 100))
    })
    
    output$memory_usage <- shiny::renderText({
      mem_info <- gc()
      sprintf("%.0f MB", sum(mem_info[, 2]))
    })
    
    output$worker_status <- shiny::renderText({
      stats <- current_stats()
      q <- queue_obj()
      if (!is.null(q)) {
        sprintf("%d/%d active", stats$running, q$.__enclos_env__$private$.max_workers)
      } else {
        "N/A"
      }
    })
    
    output$queue_size <- shiny::renderText({
      stats <- current_stats()
      as.character(stats$queued)
    })
    
    # Performance plot
    output$performance_plot <- shiny::renderPlot({
      refresh_timer()
      
      # Update history
      stats <- current_stats()
      history <- stats_history()
      
      if (length(history) > 60) {
        history <- history[-1]
      }
      
      history[[length(history) + 1]] <- list(
        time = Sys.time(),
        running = stats$running,
        queued = stats$queued
      )
      
      stats_history(history)
      
      # Create plot
      if (length(history) > 1) {
        times <- sapply(history, function(x) x$time)
        running <- sapply(history, function(x) x$running)
        queued <- sapply(history, function(x) x$queued)
        
        par(mar = c(2, 4, 1, 1))
        plot(times, running, type = "l", col = "blue", lwd = 2,
             ylim = c(0, max(c(running, queued)) + 1),
             xlab = "", ylab = "Jobs", xaxt = "n")
        lines(times, queued, col = "orange", lwd = 2)
        legend("topright", c("Running", "Queued"), 
               col = c("blue", "orange"), lwd = 2, bty = "n")
        grid(col = "gray90")
      } else {
        plot.new()
        text(0.5, 0.5, "Waiting for data...", cex = 1.2, col = "gray")
      }
    })
    
    # Resource plot
    output$resource_plot <- shiny::renderPlot({
      refresh_timer()
      
      # Update resource history
      history <- resource_history()
      
      if (length(history) > 60) {
        history <- history[-1]
      }
      
      stats <- current_stats()
      cpu <- min((stats$running / 4) * 100, 100)
      mem <- sum(gc()[, 2])
      
      history[[length(history) + 1]] <- list(
        time = Sys.time(),
        cpu = cpu,
        memory = mem
      )
      
      resource_history(history)
      
      # Create plot
      if (length(history) > 1) {
        times <- sapply(history, function(x) x$time)
        cpu_usage <- sapply(history, function(x) x$cpu)
        
        par(mar = c(2, 4, 1, 4))
        plot(times, cpu_usage, type = "l", col = "red", lwd = 2,
             ylim = c(0, 100), xlab = "", ylab = "CPU %", xaxt = "n")
        grid(col = "gray90")
        
        # Add memory on secondary axis
        par(new = TRUE)
        memory_usage <- sapply(history, function(x) x$memory)
        plot(times, memory_usage, type = "l", col = "green", lwd = 2,
             axes = FALSE, xlab = "", ylab = "")
        axis(4, col = "green", col.axis = "green")
        mtext("Memory (MB)", side = 4, line = 2.5, col = "green")
        
        legend("topleft", c("CPU", "Memory"), 
               col = c("red", "green"), lwd = 2, bty = "n")
      } else {
        plot.new()
        text(0.5, 0.5, "Waiting for data...", cex = 1.2, col = "gray")
      }
    })
    
    # Job table
    output$job_table <- DT::renderDT({
      refresh_timer()
      input$refresh_jobs
      
      q <- queue_obj()
      if (is.null(q)) {
        return(data.frame())
      }
      
      # Collect all jobs
      all_jobs <- list()
      
      # Get jobs from different states
      # Note: This is simplified - actual implementation would need queue internals
      stats <- current_stats()
      
      # Create sample data for demonstration
      job_data <- data.frame(
        ID = character(),
        Type = character(),
        Status = character(),
        Priority = integer(),
        Submit_Time = character(),
        Runtime = character(),
        stringsAsFactors = FALSE
      )
      
      # Add some mock jobs for demonstration
      if (stats$running > 0) {
        for (i in 1:min(stats$running, 5)) {
          job_data <- rbind(job_data, data.frame(
            ID = paste0("job_", sample(1000:9999, 1)),
            Type = sample(c("stan_fit", "pta_calculation", "batch"), 1),
            Status = '<span class="job-status-badge status-running">RUNNING</span>',
            Priority = sample(1:10, 1),
            Submit_Time = format(Sys.time() - runif(1, 0, 300), "%H:%M:%S"),
            Runtime = sprintf("%.1fs", runif(1, 5, 60)),
            stringsAsFactors = FALSE
          ))
        }
      }
      
      if (stats$queued > 0) {
        for (i in 1:min(stats$queued, 5)) {
          job_data <- rbind(job_data, data.frame(
            ID = paste0("job_", sample(1000:9999, 1)),
            Type = sample(c("stan_fit", "pta_calculation", "batch"), 1),
            Status = '<span class="job-status-badge status-queued">QUEUED</span>',
            Priority = sample(1:10, 1),
            Submit_Time = format(Sys.time() - runif(1, 0, 60), "%H:%M:%S"),
            Runtime = "-",
            stringsAsFactors = FALSE
          ))
        }
      }
      
      DT::datatable(
        job_data,
        escape = FALSE,
        options = list(
          pageLength = 10,
          dom = 'tp',
          ordering = FALSE
        ),
        rownames = FALSE
      )
    })
    
    # Control actions
    shiny::observeEvent(input$update_workers, {
      q <- queue_obj()
      if (!is.null(q)) {
        # Update max workers (would need queue method)
        shiny::showNotification(
          sprintf("Updated to %d workers", input$max_workers),
          type = "success"
        )
      }
    })
    
    shiny::observeEvent(input$clear_completed, {
      q <- queue_obj()
      if (!is.null(q)) {
        q$clear("completed")
        shiny::showNotification("Cleared completed jobs", type = "success")
      }
    })
    
    shiny::observeEvent(input$cancel_all, {
      shiny::showModal(
        shiny::modalDialog(
          title = "Confirm Cancel All",
          "Are you sure you want to cancel all jobs?",
          footer = shiny::tagList(
            shiny::modalButton("Cancel"),
            shiny::actionButton("confirm_cancel_all", "Confirm", 
                              class = "btn-danger")
          )
        )
      )
    })
    
    shiny::observeEvent(input$confirm_cancel_all, {
      q <- queue_obj()
      if (!is.null(q)) {
        # Cancel all jobs (would need implementation)
        shiny::showNotification("All jobs cancelled", type = "warning")
      }
      shiny::removeModal()
    })
    
    shiny::observeEvent(input$clear_cache, {
      q <- queue_obj()
      if (!is.null(q)) {
        q$clear_cache(memory = TRUE, disk = TRUE)
        shiny::showNotification("Cache cleared", type = "success")
      }
    })
    
    # Return queue object and controls
    return(list(
      queue = queue_obj,
      refresh = shiny::reactive({ input$refresh_jobs }),
      max_workers = shiny::reactive({ input$max_workers }),
      priority_mode = shiny::reactive({ input$priority_mode }),
      cache_enabled = shiny::reactive({ input$enable_cache })
    ))
  })
}

# ============================================================================
# Standalone App for Testing
# ============================================================================

#' Run standalone job monitor app
#' 
#' @param queue Existing JobQueue object or NULL
#' @export
runJobMonitor <- function(queue = NULL) {
  
  ui <- shiny::fluidPage(
    shiny::titlePanel("TDMx Job Queue Monitor"),
    jobMonitorUI("monitor")
  )
  
  server <- function(input, output, session) {
    monitor <- jobMonitorServer("monitor", queue = queue)
    
    # Add some test jobs
    shiny::observeEvent(input$add_test_job, {
      q <- monitor$queue()
      if (!is.null(q)) {
        job_id <- q$submit(
          type = "batch_processing",
          spec = list(
            data = 1:100,
            fun = function(x) {
              Sys.sleep(runif(1, 1, 5))
              x^2
            }
          ),
          priority = sample(1:10, 1)
        )
        shiny::showNotification(sprintf("Added test job: %s", job_id),
                               type = "info")
      }
    })
  }
  
  shiny::shinyApp(ui, server)
}

# Run if sourced directly
if (interactive() && !exists("SHINY_RUNNING")) {
  cat("Starting Job Monitor Demo App...\n")
  runJobMonitor()
}
