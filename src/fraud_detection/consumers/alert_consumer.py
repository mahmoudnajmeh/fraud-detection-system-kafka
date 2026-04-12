"""Alert consumer with rich terminal UI - Avro version."""

from confluent_kafka import Consumer, KafkaError
import sys
import os
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text
from collections import deque
import signal
from pathlib import Path
from loguru import logger

sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from fraud_detection.config.settings import settings
from fraud_detection.config.logger_config import logger
from fraud_detection.utils.avro_serializer import AvroSerializer

console = Console()

class AlertConsumer:
    """Consumer that displays fraud alerts in real-time with a rich UI."""
    
    def __init__(self):
        """Initialize the alert consumer."""
        self.consumer_config = {
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': settings.ALERT_CONSUMER_GROUP,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
        }
        
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([settings.FRAUD_ALERT_TOPIC])
        
        alert_schema_path = Path(__file__).parent.parent / "schemas" / "fraud_alert.avsc"
        self.alert_serializer = AvroSerializer(str(alert_schema_path))
        
        self.recent_alerts = deque(maxlen=50)
        self.running = True
        self.alert_count = 0
        self.severity_counts = {'CRITICAL': 0, 'HIGH': 0, 'MEDIUM': 0, 'LOW': 0}
        self.type_counts = {}
        
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        logger.info("Alert Consumer initialized (Avro)")
    
    def signal_handler(self, sig, frame):
        """Handle shutdown signals."""
        logger.info("Shutdown signal received")
        self.running = False
    
    def process_alert(self, alert_data: bytes):
        """Process and display a fraud alert."""
        try:
            records = self.alert_serializer.deserialize(alert_data)
            if not records:
                return
            
            alert = records[0]
            alert['display_time'] = datetime.now().strftime("%H:%M:%S")
            self.recent_alerts.appendleft(alert)
            self.alert_count += 1
            
            severity = alert.get('severity', 'LOW')
            if severity in self.severity_counts:
                self.severity_counts[severity] += 1
            
            alert_type = alert.get('alert_type', 'UNKNOWN')
            self.type_counts[alert_type] = self.type_counts.get(alert_type, 0) + 1
            
            logger.bind(audit=True).info(f"Alert: {alert['alert_type']} - {alert['description']}")
            
        except Exception as e:
            logger.error(f"Error processing alert: {e}")
    
    def generate_display(self):
        """Generate the rich display layout."""
        layout = Layout()
        
        layout.split(
            Layout(name="header", size=3),
            Layout(name="body")
        )
        
        layout["body"].split_row(
            Layout(name="summary", size=35),
            Layout(name="alerts")
        )
        
        header_text = Text("🚨 FRAUD DETECTION SYSTEM - REAL-TIME ALERTS 🚨", style="bold red on white")
        layout["header"].update(Panel(header_text, style="bold"))
        
        summary_content = self.generate_summary()
        layout["summary"].update(Panel(summary_content, title="📊 Statistics", border_style="blue"))
        
        alerts_content = self.generate_alerts_table()
        layout["alerts"].update(Panel(alerts_content, title="📋 Recent Alerts", border_style="red"))
        
        return layout
    
    def generate_summary(self):
        """Generate summary statistics."""
        summary = Text()
        
        summary.append(f"\nTotal Alerts: {self.alert_count}\n\n", style="bold cyan")
        
        summary.append("Alert Severity:\n", style="bold yellow")
        summary.append(f"  🔴 CRITICAL: {self.severity_counts['CRITICAL']}\n")
        summary.append(f"  🟡 HIGH: {self.severity_counts['HIGH']}\n")
        summary.append(f"  🟠 MEDIUM: {self.severity_counts['MEDIUM']}\n")
        summary.append(f"  🟢 LOW: {self.severity_counts['LOW']}\n")
        
        if self.type_counts:
            summary.append("\nAlert Types:\n", style="bold yellow")
            for alert_type, count in sorted(self.type_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                summary.append(f"  • {alert_type}: {count}\n")
        
        return summary
    
    def generate_alerts_table(self):
        """Generate table of recent alerts."""
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Time", width=10, style="cyan")
        table.add_column("Severity", width=10)
        table.add_column("Type", width=18)
        table.add_column("Description", width=50)
        
        if not self.recent_alerts:
            table.add_row("", "", "", "No alerts yet...")
            return table
        
        for alert in list(self.recent_alerts)[:15]:
            severity = alert['severity']
            severity_display = {
                'CRITICAL': '🔴 CRITICAL',
                'HIGH': '🟡 HIGH',
                'MEDIUM': '🟠 MEDIUM',
                'LOW': '🟢 LOW'
            }.get(severity, severity)
            
            description = alert['description']
            if len(description) > 47:
                description = description[:47] + "..."
            
            table.add_row(
                alert['display_time'],
                severity_display,
                alert['alert_type'],
                description
            )
        
        return table
    
    def run(self):
        """Main consumer loop with rich live display."""
        logger.info("Starting Alert Consumer with UI...")
        
        os.system('clear' if os.name == 'posix' else 'cls')
        
        with Live(self.generate_display(), refresh_per_second=2, screen=True) as live:
            try:
                while self.running:
                    msg = self.consumer.poll(timeout=0.5)
                    
                    if msg is None:
                        live.update(self.generate_display())
                        continue
                    
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            logger.error(f"Consumer error: {msg.error()}")
                        continue
                    
                    self.process_alert(msg.value())
                    live.update(self.generate_display(), refresh=True)
                    
            except KeyboardInterrupt:
                logger.info("Shutting down...")
            finally:
                self.consumer.close()
                logger.success("Alert Consumer stopped")