#!/usr/bin/env python3
"""
IV Interpolation Pipeline Setup Script
Run this script to set up the project environment and validate the configuration.
"""

import os
import sys
from pathlib import Path
import subprocess
import shutil

def print_banner():
    print("""
╔══════════════════════════════════════════════════════════════╗
║                 IV Interpolation Pipeline                    ║
║                      Setup Script                           ║
╚══════════════════════════════════════════════════════════════╝
""")

def check_python_version():
    """Check if Python version is compatible"""
    print("🐍 Checking Python version...")
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        print(f"   Current version: {sys.version}")
        return False
    print(f"✅ Python {sys.version.split()[0]} detected")
    return True

def create_directory_structure():
    """Create the required directory structure"""
    print("📁 Creating directory structure...")
    
    directories = [
        "src",
        "src/database",
        "src/interpolation", 
        "src/monitoring",
        "logs",
        "interpolated_data",
        "tests"
    ]
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"   ✅ {directory}/")
    
    # Create __init__.py files for proper Python modules
    init_files = [
        "src/__init__.py",
        "src/database/__init__.py",
        "src/interpolation/__init__.py",
        "src/monitoring/__init__.py"
    ]
    
    for init_file in init_files:
        Path(init_file).touch()
    
    print("✅ Directory structure created")

def setup_virtual_environment():
    """Set up Python virtual environment"""
    print("🌐 Setting up virtual environment...")
    
    venv_path = Path(".venv")
    
    if venv_path.exists():
        print("   ⚠️  Virtual environment already exists")
        response = input("   Remove and recreate? (y/N): ")
        if response.lower() == 'y':
            shutil.rmtree(venv_path)
        else:
            print("   Skipping virtual environment setup")
            return True
    
    try:
        # Create virtual environment
        subprocess.run([sys.executable, "-m", "venv", ".venv"], check=True)
        print("   ✅ Virtual environment created")
        
        # Determine activation script path
        if os.name == 'nt':  # Windows
            activate_script = ".venv\\Scripts\\activate"
            pip_path = ".venv\\Scripts\\pip"
        else:  # Unix/Linux/macOS
            activate_script = ".venv/bin/activate"
            pip_path = ".venv/bin/pip"
        
        print(f"   💡 To activate: source {activate_script}")
        
        # Install requirements if they exist
        if Path("requirements.txt").exists():
            print("   📦 Installing requirements...")
            subprocess.run([pip_path, "install", "-r", "requirements.txt"], check=True)
            print("   ✅ Requirements installed")
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Failed to set up virtual environment: {e}")
        return False

def create_env_template():
    """Create .env template file"""
    print("🔧 Creating environment configuration...")
    
    env_template = """# Database Configuration
DB_HOST=localhost
DB_DATABASE=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
DB_PORT=5432

# Processing Environment
ENVIRONMENT=development
"""
    
    env_file = Path(".env")
    if env_file.exists():
        print("   ⚠️  .env file already exists")
        return True
    
    with open(env_file, 'w') as f:
        f.write(env_template)
    
    print("   ✅ .env template created")
    print("   💡 Please edit .env with your database credentials")
    return True

def update_requirements():
    """Create a proper requirements.txt file"""
    print("📦 Creating requirements.txt...")
    
    requirements = """# Core data processing
pandas>=1.5.0
numpy>=1.24.0
scipy>=1.10.0

# Database
psycopg2-binary>=2.9.0

# Performance and parallelization
numba>=0.56.0
joblib>=1.2.0

# Utilities
python-dateutil>=2.8.0
pytz>=2022.1
python-dotenv>=1.0.0

# Development tools (optional)
pytest>=7.0.0
pytest-cov>=4.0.0
black>=22.0.0
flake8>=5.0.0
"""
    
    with open("requirements.txt", 'w') as f:
        f.write(requirements)
    
    print("   ✅ requirements.txt updated")

def validate_configuration():
    """Validate that the configuration is correct"""
    print("🔍 Validating configuration...")
    
    # Check if .env exists and has required variables
    env_file = Path(".env")
    if not env_file.exists():
        print("   ❌ .env file not found")
        return False
    
    # Try to load environment variables
    try:
        from dotenv import load_dotenv
        load_dotenv()
        
        required_vars = ['DB_HOST', 'DB_DATABASE', 'DB_USER', 'DB_PASSWORD']
        missing_vars = []
        
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            print(f"   ❌ Missing environment variables: {', '.join(missing_vars)}")
            print("   💡 Please update your .env file with database credentials")
            return False
        
        print("   ✅ Environment variables configured")
        
    except ImportError:
        print("   ⚠️  python-dotenv not installed - run: pip install python-dotenv")
        return False
    except Exception as e:
        print(f"   ❌ Configuration validation failed: {e}")
        return False
    
    return True

def test_database_connection():
    """Test database connection"""
    print("🗄️  Testing database connection...")
    
    try:
        # Add src to path for imports
        sys.path.append(str(Path("src")))
        
        from config import get_config
        from database.connection import DatabaseManager
        
        config = get_config()
        db_manager = DatabaseManager(config.database)
        
        # Test connection
        with db_manager.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                print(f"   ✅ Connected to PostgreSQL: {version.split(',')[0]}")
        
        return True
        
    except ImportError as e:
        print(f"   ⚠️  Cannot test database - missing dependencies: {e}")
        print("   💡 Install requirements and try again")
        return False
    except Exception as e:
        print(f"   ❌ Database connection failed: {e}")
        print("   💡 Check your .env file and ensure PostgreSQL is running")
        return False

def show_next_steps():
    """Show what to do next"""
    print("""
🎯 SETUP COMPLETE! Next Steps:

1. 📝 Edit .env file with your database credentials:
   - Update DB_HOST, DB_DATABASE, DB_USER, DB_PASSWORD

2. 🗄️  Ensure your PostgreSQL database contains the 'trading_tickers' table
   with your source data

3. 🧪 Run a test to validate everything works:
   python main.py --test

4. 🚀 Run the full pipeline:
   python main.py

5. 📊 Monitor progress:
   python main.py --monitor

📚 Available Commands:
   python main.py --help                    # Show all options
   python main.py --test                    # Test with small dataset  
   python main.py --validate-only           # Check database setup
   python main.py --list-batches            # Show recent batches
   python main.py --resume <batch_id>       # Resume failed batch
   python main.py --env development         # Use development settings

💡 Tips:
   - Use 'development' environment for testing
   - Check logs/ directory for detailed logging
   - Use --monitor to watch real-time progress
   - Failed batches can be resumed using batch ID

Happy interpolating! 🚀
""")

def main():
    """Main setup function"""
    print_banner()
    
    steps = [
        ("Checking Python version", check_python_version),
        ("Creating directory structure", create_directory_structure),
        ("Updating requirements", update_requirements),
        ("Setting up virtual environment", setup_virtual_environment),
        ("Creating environment template", create_env_template),
        ("Validating configuration", validate_configuration),
        ("Testing database connection", test_database_connection),
    ]
    
    failed_steps = []
    
    for step_name, step_func in steps:
        print(f"\n{'='*60}")
        try:
            if not step_func():
                failed_steps.append(step_name)
        except Exception as e:
            print(f"❌ {step_name} failed: {e}")
            failed_steps.append(step_name)
    
    print(f"\n{'='*60}")
    print("🏁 SETUP SUMMARY")
    
    if failed_steps:
        print(f"❌ {len(failed_steps)} steps failed:")
        for step in failed_steps:
            print(f"   - {step}")
        print("\n💡 Please resolve the issues above and run setup again")
        sys.exit(1)
    else:
        print("✅ All setup steps completed successfully!")
        show_next_steps()

if __name__ == "__main__":
    main()