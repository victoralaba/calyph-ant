# domains/notifications/tasks.py
from celery import shared_task
from celery.utils.log import get_task_logger
import asyncio

logger = get_task_logger(__name__)

def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

@shared_task(
    bind=True,
    name="notifications.send_async_email",
    max_retries=3,
    default_retry_delay=60, # Retry after 1 min if SendPulse is unreachable
    queue="notifications"
)
def send_async_email(
    self, 
    to_email: str, 
    to_name: str, 
    subject: str, 
    html_content: str, 
    sender_type: str = "system"
):
    """
    Celery task to dispatch emails off the main FastAPI event loop.
    """
    from domains.notifications.emails import send_sendpulse_email
    
    try:
        success = _run(send_sendpulse_email(
            to_email=to_email,
            to_name=to_name,
            subject=subject,
            html_content=html_content,
            sender_type=sender_type
        ))
        
        if not success:
            raise Exception("SendPulse returned False/Failure.")
            
    except Exception as exc:
        logger.error(f"Async email failed for {to_email}: {exc}. Retrying...")
        self.retry(exc=exc)