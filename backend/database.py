from sqlalchemy import create_engine, Column, Integer, String, Decimal, ForeignKey, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import pyodbc
import urllib
import os
from datetime import datetime

# MS SQL Server connection for 192.168.0.242
CONNECTION_STRING = "mssql+pyodbc://sa:8308chris@192.168.0.242/StockDB?driver=ODBC+Driver+17+for+SQL+Server"

class SQLServerConnection:
    def __init__(self):
        self.engine = create_engine(CONNECTION_STRING, echo=True)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def get_session(self):
        return self.SessionLocal()
    
    def test_connection(self):
        """Test database connectivity"""
        try:
            with self.engine.connect() as connection:
                result = connection.execute("SELECT 1")
                return result.scalar() == 1
        except Exception as e:
            print(f"Database connection error: {e}")
            return False

Base = declarative_base()

# Database models with proper T-SQL compatibility

class Portfolio(Base):
    __tablename__ = 'portfolios'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    created_date = Column(DateTime, default=datetime.utcnow)
    updated_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    total_value = Column(Decimal(15, 2), default=0.00)
    
    # Relationships
    holdings = relationship("StockHolding", back_populates="portfolio")
    transactions = relationship("Transaction", back_populates="portfolio")

class Stock(Base):
    __tablename__ = 'stocks'
    
    symbol = Column(String(10), primary_key=True)
    name = Column(String(255))
    sector = Column(String(100))
    industry = Column(String(100))
    market_cap = Column(String(50))
    last_price = Column(Decimal(10, 2))
    price_updated_at = Column(DateTime)
    currency = Column(String(3), default='USD')
    
    # Relationships
    holdings = relationship("StockHolding", back_populates="stock")
    prices = relationship("StockPrice", back_populates="stock")

class StockHolding(Base):
    __tablename__ = 'stock_holdings'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    portfolio_id = Column(Integer, ForeignKey('portfolios.id'), nullable=False)
    symbol = Column(String(10), ForeignKey('stocks.symbol'), nullable=False)
    quantity = Column(Integer, nullable=False)
    purchase_price = Column(Decimal(10, 2), nullable=False)
    purchase_date = Column(DateTime, default=datetime.utcnow)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    portfolio = relationship("Portfolio", back_populates="holdings")
    stock = relationship("Stock", back_populates="holdings")

class Transaction(Base):
    __tablename__ = 'transactions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    portfolio_id = Column(Integer, ForeignKey('portfolios.id'), nullable=False)
    symbol = Column(String(10), ForeignKey('stocks.symbol'), nullable=False)
    transaction_type = Column(String(10))  # BUY, SELL, DIVIDEND
    quantity = Column(Integer)
    price_per_unit = Column(Decimal(10, 2))
    total_amount = Column(Decimal(15, 2))
    fees = Column(Decimal(10, 2), default=0.00)
    transaction_date = Column(DateTime, default=datetime.utcnow)
    notes = Column(Text)
    
    # Relationships
    portfolio = relationship("Portfolio", back_populates="transactions")
    stock = relationship("Stock")

class StockPrice(Base):
    __tablename__ = 'stock_prices'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), ForeignKey('stocks.symbol'), nullable=False)
    price = Column(Decimal(10, 2), nullable=False)
    change_percent = Column(Decimal(5, 4))
    volume = Column(Integer)
    timestamp = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    stock = relationship("Stock", back_populates="prices")

# Initialize database connection
db_connection = SQLServerConnection()

# Create tables if they don't exist
Base.metadata.create_all(db_connection.engine)

# Database utilities
class DatabaseManager:
    def __init__(self):
        self.connection = db_connection
    
    def get_portfolios(self):
        """Get all portfolios with their current value"""
        with self.connection.get_session() as session:
            return session.query(Portfolio).all()
    
    def get_portfolio_by_id(self, portfolio_id: int):
        """Get a single portfolio by ID"""
        with self.connection.get_session() as session:
            return session.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
    
    def create_portfolio(self, name: str, description: str = None):
        """Create a new portfolio"""
        with self.connection.get_session() as session:
            portfolio = Portfolio(name=name, description=description)
            session.add(portfolio)
            session.commit()
            session.refresh(portfolio)
            return portfolio
    
    def add_stock_to_portfolio(self, portfolio_id: int, symbol: str, quantity: int, purchase_price: float):
        """Add a stock to a portfolio"""
        with self.connection.get_session() as session:
            # Ensure stock exists
            stock = session.query(Stock).filter(Stock.symbol == symbol).first()
            if not stock:
                stock = Stock(symbol=symbol, name=symbol)  # Simplified
                session.add(stock)
                session.commit()
            
            holding = StockHolding(
                portfolio_id=portfolio_id,
                symbol=symbol,
                quantity=quantity,
                purchase_price=purchase_price
            )
            session.add(holding)
            session.commit()
            return holding
    
    def update_portfolio_value(self, portfolio_id: int):
        """Calculate and update portfolio total value based on current stock prices"""
        with self.connection.get_session() as session:
            # Get portfolio and its holdings
            portfolio = session.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
            if not portfolio:
                return None
            
            # Calculate total value
            total_value = 0
            for holding in portfolio.holdings:
                # Get current price (simplified - in real implementation use API)
                stock = session.query(Stock).filter(Stock.symbol == holding.symbol).first()
                if stock and stock.last_price:
                    current_value = holding.quantity * float(stock.last_price)
                    total_value += current_value
            
            # Update portfolio total value
            portfolio.total_value = total_value
            session.commit()
            return total_value
    
    def seed_initial_data(self):
        """Seed initial test data"""
        with self.connection.get_session() as session:
            # Create test portfolios
            portfolios = [
                Portfolio(name="Tech Giants", description="Large cap technology stocks"),
                Portfolio(name="Dividend Fund", description="Stable dividend-paying stocks"),
                Portfolio(name="Growth Portfolio", description="High-growth potential stocks")
            ]
            
            # Create stocks
            stocks = [
                Stock(symbol="AAPL", name="Apple Inc.", sector="Technology"),
                Stock(symbol="MSFT", name="Microsoft Corporation", sector="Technology"),
                Stock(symbol="GOOGL", name="Alphabet Inc.", sector="Technology"),
                Stock(symbol="TSLA", name="Tesla Inc.", sector="Automotive")
            ]
            
            session.add_all(portfolios + stocks)
            session.commit()
            
            # Add some initial prices (mock data)
            for symbol in ["AAPL", "MSFT", "GOOGL", "TSLA"]:
                price = StockPrice(
                    symbol=symbol,
                    price=100.00,  # Mock price
                    change_percent=0.5,
                    volume=1000000
                )
                session.add(price)
            session.commit()
            
            return True

# Initialize and test connection
if __name__ == "__main__":
    from database import DatabaseManager
    db_manager = DatabaseManager()
    
    # Test connection
    if db_connection.test_connection():
        print("✅ Database connection to StockDB successful!")
        
        # Seed initial data
        db_manager.seed_initial_data()
        print("✅ Initial data seeded successfully!")
    else:
        print("❌ Database connection failed. Check server 192.168.0.242")