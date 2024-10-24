import nextcord
from nextcord import Interaction, SlashOption
from nextcord.ext import commands
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import pytz
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

POSTGRES_URI = os.getenv('POSTGRES_URI')
BOT_TOKEN = os.getenv('BOT_TOKEN')
SCHEMA_NAME = 'bot_schema'  # Using a dedicated schema

class MessageTracker(commands.Bot):
    def __init__(self):
        intents = nextcord.Intents.default()
        intents.message_content = True
        intents.members = True
        super().__init__(intents=intents)
        
        # Initialize database connection
        self.db = Database()
        
    async def on_ready(self):
        print(f'Bot is ready! Logged in as {self.user.name}')
        try:
            # Ensure the database tables are ready
            self.db.setup_database()
            print("Database setup completed successfully!")
        except Exception as e:
            print(f"Error setting up database: {str(e)}")

    async def on_message(self, message):
        if message.author.bot:
            return
            
        try:
            # Log the message details to the database
            self.db.track_message(
                user_id=message.author.id,
                username=str(message.author),
                guild_id=message.guild.id,
                timestamp=message.created_at
            )
        except Exception as e:
            print(f"Error tracking message: {str(e)}")
        await super().on_message(message)

class Database:
    def __init__(self):
        try:
            self.conn = psycopg2.connect(POSTGRES_URI)
            self.conn.autocommit = True
            
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name = %s;
                """, (SCHEMA_NAME,))
                
                if not cur.fetchone():
                    print(f"Creating schema {SCHEMA_NAME}...")
                    cur.execute(f"CREATE SCHEMA {SCHEMA_NAME};")
                    print(f"Schema {SCHEMA_NAME} created successfully!")
                
                cur.execute(f"SET search_path TO {SCHEMA_NAME};")
            
            self.conn.autocommit = False
            
        except Exception as e:
            print(f"Error in database initialization: {str(e)}")
            raise
        
    def setup_database(self):
        with self.conn.cursor() as cur:
            try:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS messages (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        username TEXT NOT NULL,
                        guild_id BIGINT NOT NULL,
                        timestamp TIMESTAMP WITH TIME ZONE NOT NULL
                    );
                """)
                
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_messages_user_id ON messages(user_id);
                    CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp);
                    CREATE INDEX IF NOT EXISTS idx_messages_guild_id ON messages(guild_id);
                """)
                
                self.conn.commit()
                print("Tables and indexes created successfully!")
                
            except Exception as e:
                self.conn.rollback()
                print(f"Error creating tables: {str(e)}")
                raise
            
    def track_message(self, user_id: int, username: str, guild_id: int, timestamp: datetime):
        with self.conn.cursor() as cur:
            try:
                cur.execute("""
                    INSERT INTO messages (user_id, username, guild_id, timestamp)
                    VALUES (%s, %s, %s, %s)
                """, (user_id, username, guild_id, timestamp))
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                print(f"Error tracking message: {str(e)}")
                raise
            
    def get_user_statistics(self, user_id: int, guild_id: int) -> dict:
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            now = datetime.now(pytz.UTC)
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            yesterday_start = today_start - timedelta(days=1)
            week_ago = now - timedelta(days=7)
            month_ago = now - timedelta(days=30)
            
            cur.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE timestamp >= %s) as today_messages,
                    COUNT(*) FILTER (WHERE timestamp >= %s AND timestamp < %s) as yesterday_messages,
                    COUNT(*) FILTER (WHERE timestamp >= %s) as week_messages,
                    COUNT(*) FILTER (WHERE timestamp >= %s) as month_messages,
                    COUNT(*) as total_messages
                FROM messages 
                WHERE user_id = %s AND guild_id = %s
            """, (today_start, yesterday_start, today_start, week_ago, month_ago, user_id, guild_id))
            
            return cur.fetchone()
            
    def get_leaderboard(self, guild_id: int, limit: int = 10) -> list:
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT user_id, username, COUNT(*) as message_count
                FROM messages
                WHERE guild_id = %s
                GROUP BY user_id, username
                ORDER BY message_count DESC
                LIMIT %s
            """, (guild_id, limit))
            
            return cur.fetchall()

bot = MessageTracker()

@bot.slash_command(
    name="statistics",
    description="Show message statistics for a user"
)
async def statistics(
    interaction: Interaction,
    user: nextcord.Member = SlashOption(required=False, description="User to show statistics for")
):
    if user is None:
        user = interaction.user
        
    try:
        stats = bot.db.get_user_statistics(user.id, interaction.guild_id)
        
        embed = nextcord.Embed(
            title=f"Message Statistics for {user.display_name}",
            color=nextcord.Color.blue()
        )
        
        embed.set_thumbnail(url=user.display_avatar.url)
        
        embed.add_field(name="Today", value=str(stats['today_messages']), inline=True)
        embed.add_field(name="Yesterday", value=str(stats['yesterday_messages']), inline=True)
        embed.add_field(name="\u200b", value="\u200b", inline=True)
        
        embed.add_field(name="Last 7 days", value=str(stats['week_messages']), inline=True)
        embed.add_field(name="Last 30 days", value=str(stats['month_messages']), inline=True)
        embed.add_field(name="Total", value=str(stats['total_messages']), inline=True)
        

        embed.set_footer(text=f"Requested by {interaction.user} • {datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M UTC')}", icon_url=interaction.user.avatar.url)
        
        await interaction.response.send_message(embed=embed)
    except Exception as e:
        await interaction.response.send_message(f"Error retrieving statistics: {str(e)}", ephemeral=True)

@bot.slash_command(
    name="leaderboard",
    description="Show the top message senders in the server"
)
async def leaderboard(interaction: Interaction):
    try:
        leaders = bot.db.get_leaderboard(interaction.guild_id)

        embed = nextcord.Embed(
            title="🏆 Server Leaderboard",
            description="**Top 10 Most Active Users**",
            color=nextcord.Color.gold(),
            timestamp=nextcord.utils.utcnow()
        )

        medals = ["🥇", "🥈", "🥉"]
        for i, leader in enumerate(leaders, 1):
            medal = medals[i-1] if i <= 3 else f"#{i}"
            embed.add_field(
                name=f"{medal} {leader['username']}",
                value=f"**{leader['message_count']}** messages",
                inline=False
            )
        
        embed.set_footer(text=f"Requested by {interaction.user}", icon_url=interaction.user.avatar.url)

        await interaction.response.send_message(embed=embed)
    
    except Exception as e:
        await interaction.response.send_message(f"Error retrieving leaderboard: {str(e)}", ephemeral=True)

if __name__ == "__main__":
    bot.run(BOT_TOKEN)
