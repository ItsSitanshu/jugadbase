def generate_insert_statements():
    
    event_names = [
        "Closing Presentation", "Tech Talk Networking", "Breakout Networking",
        "Pitch Session", "Hackathon Recap", "Keynote Wrap-up", "Breakout Hour",
        "Panel Start", "Hackathon Day", "Demo Presentation", "Closing Wrap-up",
        "Pitch Networking", "Workshop Presentation", "Tech Talk Day", "Hackathon Hour"
    ]
    
    
    with open("events_insert_statements.sql", "w") as file:
        
        file.write("""
            CREATE TABLE events (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            event_date DATE,
            event_time TIME,
            event_timestamp TIMESTAMP,
            event_timestamptz TIMESTAMPTZ,
            duration INTERVAL
            );

        """)
        
        for i in range(1, 1001):
            name = event_names[i % len(event_names)]  
            event_date = f"2025-05-{(i % 31) + 1:02d}"  
            event_time = f"{(i % 24):02d}:{(i % 60):02d}:00"  
            event_timestamp = f"2025-05-{(i % 31) + 1:02d} {(i % 24):02d}:{(i % 60):02d}:00"
            event_timestamptz = f"2025-05-{(i % 31) + 1:02d} {(i % 24):02d}:{(i % 60):02d}:00+{i%24}:{i%60}"
            duration = f"0 days {(i % 12) + 1} hours"  
            

            if i % 10 == 0:
                file.write(";\n")
                file.write(f"INSERT INTO events VALUES\n")
            elif i == 1:
                file.write(f"INSERT INTO events VALUES\n")
            else:
                file.write(",\n")

                

            file.write(f"({i}, \"{name}\", \"{event_date}\", \"{event_time}\", \"{event_timestamp}\", \"{event_timestamptz}\", \"{duration}\")")



if __name__ == "__main__":
    generate_insert_statements()
    print("SQL Insert statements have been generated and saved in 'events_insert_statements.sql'.")
