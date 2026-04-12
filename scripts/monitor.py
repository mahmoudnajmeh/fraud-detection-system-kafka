import time
from datetime import datetime
from pathlib import Path
from confluent_kafka.admin import AdminClient
from confluent_kafka import Producer
from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text
import psutil
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from fraud_detection.config.settings import settings

console = Console()

class KafkaMonitor:
    
    def __init__(self):
        self.admin_client = AdminClient({
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS
        })
        
        self.producer = Producer({
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS
        })
        
        self.metrics = {}
    
    def get_cluster_metadata(self):
        try:
            metadata = self.admin_client.list_topics(timeout=10)
            return metadata
        except Exception as e:
            return {'error': str(e)}
    
    def measure_latency(self):
        try:
            start = time.time()
            self.producer.produce('__monitor_test', value=b'test', callback=None)
            self.producer.flush(timeout=5)
            latency = (time.time() - start) * 1000
            return latency
        except:
            return None
    
    def get_system_stats(self):
        return {
            'cpu_percent': psutil.cpu_percent(interval=1),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_usage': psutil.disk_usage('/').percent
        }
    
    def generate_display(self):
        layout = Layout()
        
        layout.split(
            Layout(name="header", size=3),
            Layout(name="body")
        )
        
        layout["body"].split_row(
            Layout(name="cluster", size=40),
            Layout(name="system")
        )
        
        header_text = Text(
            f"KAFKA CLUSTER MONITOR - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            style="bold cyan"
        )
        layout["header"].update(Panel(header_text))
        
        metadata = self.get_cluster_metadata()
        latency = self.measure_latency()
        
        cluster_content = Text()
        if 'error' in metadata:
            cluster_content.append(f"Error: {metadata['error']}", style="red")
        else:
            brokers = metadata.brokers
            topics = metadata.topics
            
            cluster_content.append(f"Brokers: {len(brokers)}\n", style="bold")
            cluster_content.append(f"Topics: {len(topics)}\n\n", style="bold")
            
            cluster_content.append("Topic Partitions:\n", style="underline")
            for topic_name, topic in list(topics.items())[:10]:
                if topic_name.startswith('__'):
                    continue
                cluster_content.append(f"  {topic_name}: {len(topic.partitions)} partitions\n")
            
            cluster_content.append("\n")
            if latency:
                color = "green" if latency < 100 else "yellow" if latency < 500 else "red"
                cluster_content.append(f"Latency: [{color}]{latency:.2f}ms[/{color}]\n")
        
        layout["cluster"].update(Panel(cluster_content, title="Cluster Status", border_style="blue"))
        
        stats = self.get_system_stats()
        system_content = Text()
        system_content.append(f"CPU: {stats['cpu_percent']}%\n")
        system_content.append(f"Memory: {stats['memory_percent']}%\n")
        system_content.append(f"Disk: {stats['disk_usage']}%\n")
        
        layout["system"].update(Panel(system_content, title="System Resources", border_style="green"))
        
        return layout
    
    def run(self):
        with Live(self.generate_display(), refresh_per_second=2) as live:
            try:
                while True:
                    live.update(self.generate_display())
                    time.sleep(5)
            except KeyboardInterrupt:
                console.print("\n[yellow]Monitor stopped[/yellow]")

if __name__ == "__main__":
    monitor = KafkaMonitor()
    monitor.run()