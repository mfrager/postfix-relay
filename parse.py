#!/usr/bin/python3

import re
import json
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Text, JSON, Table, MetaData, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base

# Define the SQLite database
DATABASE_URL = "sqlite:///log.db"

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL, echo=False)

# Base class for ORM models
Base = declarative_base()

# Define the 'logfile' table
class LogFile(Base):
    __tablename__ = 'logfile'
    id = Column(Integer, primary_key=True, autoincrement=True)
    file_name = Column(String, unique=True, nullable=False)
    last_line = Column(Integer, nullable=False)
    byte_offset = Column(Integer, nullable=False)

# Define the 'mail' table
class Mail(Base):
    __tablename__ = 'mail'
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts = Column(DateTime, nullable=True)
    postfix_id = Column(String, unique=True, nullable=False)
    message_id = Column(String, nullable=True)
    email_to = Column(String, nullable=True)
    email_from = Column(String, nullable=True)
    status = Column(String, nullable=True)
    status_detail = Column(String, nullable=True)
    log_text = Column(Text, nullable=True)

# Create tables
Base.metadata.create_all(engine)

# Create a new session
Session = sessionmaker(bind=engine)
session = Session()

# Function to parse the Postfix log file
def parse_postfix_log(file_name, file_path):
    # Get the last line read for the specified log file
    logfile_entry = session.query(LogFile).filter_by(file_name=file_name).first()
    last_line = logfile_entry.last_line if logfile_entry else 0
    byte_offset = logfile_entry.byte_offset if logfile_entry else 0

    with open(file_path, 'r') as file:
        file.seek(byte_offset)  # Move to the last read position
        line = file.readline()
        while line:
            current_line_number = last_line
            last_line = last_line + 1
            line = line.strip()

            # Example of extracting relevant fields from log line
            postfix_id = extract_postfix_id(line)
            if not(postfix_id):
                line = file.readline()
                continue
            ts = extract_timestamp(line)
            email_to = extract_email_to(line)
            email_from = extract_email_from(line)
            message_id = extract_message_id(line)
            status = extract_status(line)
            status_detail = None
            if status:
                pts = status.split(' ', 1)
                status = pts[0].replace(',', '')
                if len(pts) > 1:
                    status_detail = pts[1]

            # If a new postfix_id is found, create a new Mail entry
            mail_entry = session.query(Mail).filter_by(postfix_id=postfix_id).first()
            if not mail_entry:
                mail_entry = Mail(postfix_id=postfix_id, log_text=line, email_to=email_to, email_from=email_from, message_id=message_id, status=status, ts=ts)
                session.add(mail_entry)
            else:
                # Update the existing Mail entry with new information
                mail_entry.log_text += "\n" + line if mail_entry.log_text else line
                if ts:
                    mail_entry.ts = ts
                if email_to:
                    mail_entry.email_to = email_to
                if email_from:
                    mail_entry.email_from = email_from
                if message_id:
                    mail_entry.message_id = message_id
                if status:
                    mail_entry.status = status
                if status_detail:
                    mail_entry.status_detail = status_detail

            # Update or create LogFile entry
            if logfile_entry:
                logfile_entry.last_line = current_line_number
                logfile_entry.byte_offset = file.tell()
            else:
                logfile_entry = LogFile(file_name=file_name, last_line=current_line_number, byte_offset=file.tell())
                session.add(logfile_entry)
            session.commit()
            line = file.readline()

def extract_postfix_id(log_line):
    # Extract postfix ID from log line using regex
    match = re.search(r'postfix/[a-z]+\[\d+\]: ([A-F0-9]+):', log_line)
    return match.group(1) if match else None

def extract_email_to(log_line):
    # Extract email ID from log line using regex
    match = re.search(r'to=<([^>]+)>', log_line)
    return match.group(1) if match else None

def extract_email_from(log_line):
    # Extract email ID from log line using regex
    match = re.search(r'from=<([^>]+)>', log_line)
    return match.group(1) if match else None

def extract_message_id(log_line):
    # Extract message ID from log line using regex
    match = re.search(r'message-id=<([^>]+)>', log_line)
    return match.group(1) if match else None

def extract_status(log_line):
    # Extract status from log line using regex
    match = re.search(r'status=([a-zA-Z]+,? .*)', log_line)
    return match.group(1) if match else None

def extract_timestamp(log_line):
    # Extract timestamp from log line and convert to datetime object
    match = re.match(r'(\w{3} \d{2} \d{2}:\d{2}:\d{2})', log_line)
    if match:
        timestamp_str = match.group(1)
        current_year = datetime.now().year
        return datetime.strptime(f"{current_year} {timestamp_str}", "%Y %b %d %H:%M:%S")
    return None

# Main function to run the script
def main():
    log_file_name = 'postfix.log'
    log_file_path = '/root/mail/logs/postfix.log'
    parse_postfix_log(log_file_name, log_file_path)

if __name__ == '__main__':
    main()

