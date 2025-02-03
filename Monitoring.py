import time
import os
import base64
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# Load the authenticated service
SCOPES = ['https://mail.google.com/']
creds = Credentials.from_authorized_user_file("token.json", SCOPES)
service = build('gmail', 'v1', credentials=creds)

def download_latest_attachment(service, sender_emails):
    user_id = "me"
    sender_query = " OR ".join([f"from:{email}" for email in sender_emails])
    query = f"({sender_query}) has:attachment is:unread"
    
    results = service.users().messages().list(userId=user_id, q=query, maxResults=10).execute()
    messages = results.get("messages", [])

    if not messages:
        print("No new attachments found.")
        return None  # **Return None if no attachment is found**

    latest_msg = None
    latest_timestamp = 0
    latest_msg_id = None

    for msg in messages:
        msg_id = msg["id"]
        message = service.users().messages().get(userId=user_id, id=msg_id).execute()
        timestamp = int(message["internalDate"])  # Extract timestamp (milliseconds)

        if timestamp > latest_timestamp:
            latest_timestamp = timestamp
            latest_msg = message
            latest_msg_id = msg_id

    if not latest_msg:
        print("No valid latest message found.")
        return None

    msg_from = next(
        (header["value"] for header in latest_msg["payload"]["headers"] if header["name"] == "From"),
        "Unknown"
    )

    file_paths = []  # **List to store file paths of downloaded attachments**

    for part in latest_msg["payload"].get("parts", []):
        if part["filename"]:  # If a file is attached
            attachment_id = part["body"]["attachmentId"]
            attachment = service.users().messages().attachments().get(
                userId=user_id, messageId=latest_msg_id, id=attachment_id
            ).execute()

            file_data = base64.urlsafe_b64decode(attachment["data"])

            # Get the timestamp in DDMMYYYY_HHMMSS format
            timestamp_str = time.strftime("%d%m%Y_%H%M%S")

            # **Define the absolute save path**
            base_path = os.path.abspath("downloads")  # Get absolute path of downloads folder
            os.makedirs(base_path, exist_ok=True)
            save_path = os.path.join(base_path, f"{timestamp_str}.pdf")

            # Save the attachment
            with open(save_path, "wb") as f:
                f.write(file_data)

            file_paths.append(save_path)  # **Store path in the list**

            print(f"Downloaded: {part['filename']} from {msg_from}")

    # **Mark email as read**
    service.users().messages().modify(
        userId=user_id,
        id=latest_msg_id,
        body={"removeLabelIds": ["UNREAD"]}
    ).execute()

    print(f"Marked email from {msg_from} as read.")

    return file_paths  # **Return list of file paths**

# **Example usage**
senders = ["advay.argade22@vit.edu", "sanjiv.ashish22@vit.edu"]

while True:
    print("\nChecking for new emails...")
    downloaded_files = download_latest_attachment(service, senders)
    
    if downloaded_files:
        print("File paths:", downloaded_files)  # **Pass this list to another system**
    
    time.sleep(600)  # **Sleep for 10 minutes**
