import argparse
import csv
import os
import random
import sys

# Sample names for realistic data generation
FIRST_NAMES = [
    "Nour", "Hassan", "Ahmed", "Fatima", "Mohammed", "Aisha", "Ali", "Zainab",
    "Ibrahim", "Hana", "Omar", "Layla", "Karim", "Amira", "Youssef", "Sara",
    "Khalid", "Dina", "Samir", "Yasmin", "Rashid", "Rania", "Tariq", "Leila",
    "Walid", "Nada", "Hamza", "Mona", "Faisal", "Rana", "Adel", "Salma",
    "Bahri", "Huda", "Jamal", "Noor", "Karim", "Raida", "Majid", "Samira",
    "Nasir", "Tarana", "Rahim", "Laila", "Jaafar", "Salma", "Rashid", "Rima",
    "James", "Mary", "Robert", "Patricia", "Michael", "Jennifer", "William", "Linda",
    "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah",
    "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa", "Matthew", "Betty"
] * 100

LAST_NAMES = [
    "Hassan", "Ahmed", "Ali", "Ibrahim", "Mohammed", "Abdullah", "Rahman", "Khan",
    "Hussain", "Malik", "Hassan", "Omar", "Saleh", "Khalil", "Amin", "Nasser",
    "Saeed", "Karim", "Aziz", "Hamad", "Rashid", "Waleed", "Fahad", "Tariq",
    "Youssef", "Salem", "Jamal", "Hani", "Samir", "Rafiq", "Adel", "Faisal",
    "Badr", "Nabil", "Jalal", "Izzat", "Kadir", "Latif", "Mahdi", "Nasri",
    "Qadir", "Rauf", "Sameh", "Talal", "Uddin", "Vargas", "Walid", "Yazid",
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas"
] * 100

DEPARTMENTS = ["Engineering", "HR", "Finance", "Sales", "Marketing", "IT", "Operations"]


def generate_employees(num_employees, output_file, seed=None):
    """
    Generate employee records and write to CSV file.
    Uses streaming writes to handle large datasets efficiently.
    
    Args:
        num_employees: Number of employee records to generate
        output_file: Path to output employees.csv file
        seed: Random seed for reproducibility
    
    Returns:
        Number of employees generated
    """
    if seed is not None:
        random.seed(seed)
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
    
    with open(output_file, 'w', newline='', buffering=65536) as f:
        writer = csv.writer(f)
        # Write header
        writer.writerow(['employeeId', 'firstName', 'lastName', 'department'])
        
        # Generate and write employee records
        for i in range(1, num_employees + 1):
            emp_id = f"EMP{i:06d}"
            first_name = random.choice(FIRST_NAMES)
            last_name = random.choice(LAST_NAMES)
            department = random.choice(DEPARTMENTS)
            
            writer.writerow([emp_id, first_name, last_name, department])
            
            # Progress reporting
            if i % 100000 == 0:
                print(f"Generated {i:,} employee records...", file=sys.stderr)
    
    return num_employees


def generate_payroll(num_payroll_records, num_employees, output_file, bad_record_rate=0.01, seed=None, target_size_gb=None):
    """
    Generate payroll records with intentional bad data for validation testing.
    Uses streaming writes to handle large datasets efficiently.
    
    When target_size_gb is provided, generates records until file reaches target size.
    When target_size_gb is None, generates fixed number of records (num_payroll_records).
    
    Args:
        num_payroll_records: Number of payroll records to generate (ignored if target_size_gb is set)
        num_employees: Number of valid employees
        output_file: Path to output payroll.csv file
        bad_record_rate: Percentage of corrupted records (0.0 to 1.0)
        seed: Random seed for reproducibility
        target_size_gb: Target file size in GB (overrides num_payroll_records if provided)
    
    Returns:
        Tuple of (total records, bad records count)
    """
    if seed is not None:
        random.seed(seed)
    
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    bad_record_count = 0
    record_count = 0
    batch_size = 10000  # Check file size every 10k records
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
    
    # Convert target size to bytes if provided
    target_size_bytes = None
    if target_size_gb is not None:
        target_size_bytes = target_size_gb * 1024 * 1024 * 1024
    
    with open(output_file, 'w', newline='', buffering=65536) as f:
        writer = csv.writer(f)
        # Write header
        writer.writerow(['payrollId', 'employeeId', 'month', 'baseSalary', 'bonus'])
        
        # Generate payroll records
        i = 1
        
        # Determine loop condition based on whether target_size_gb is set
        if target_size_bytes is not None:
            # Loop until file size reaches target
            while True:
                # Write batch of records
                for batch_idx in range(batch_size):
                    payroll_id = f"PR{i:09d}"
                    record_count += 1
                    
                    # Determine if this should be a bad record
                    is_bad_record = random.random() < bad_record_rate
                    
                    if is_bad_record:
                        bad_record_count += 1
                        # Randomly choose type of corruption
                        bad_type = random.choice(['missing_field', 'invalid_salary', 'negative_value', 'unknown_emp_id'])
                        
                        if bad_type == 'missing_field':
                            month = random.choice(months)
                            base_salary = random.randint(3000, 15000)
                            bonus = random.randint(0, 3000)
                            writer.writerow([payroll_id, '', month, base_salary, bonus])
                            
                        elif bad_type == 'invalid_salary':
                            emp_id = f"EMP{random.randint(1, num_employees):06d}"
                            month = random.choice(months)
                            writer.writerow([payroll_id, emp_id, month, 'ABC', random.randint(0, 3000)])
                            
                        elif bad_type == 'negative_value':
                            emp_id = f"EMP{random.randint(1, num_employees):06d}"
                            month = random.choice(months)
                            base_salary = random.randint(3000, 15000)
                            bonus = -random.randint(100, 1000)
                            writer.writerow([payroll_id, emp_id, month, base_salary, bonus])
                            
                        else:  # unknown_emp_id
                            emp_id = f"EMP{random.randint(num_employees + 1, num_employees + 100000):06d}"
                            month = random.choice(months)
                            base_salary = random.randint(3000, 15000)
                            bonus = random.randint(0, 3000)
                            writer.writerow([payroll_id, emp_id, month, base_salary, bonus])
                    else:
                        emp_id = f"EMP{random.randint(1, num_employees):06d}"
                        month = random.choice(months)
                        base_salary = random.randint(3000, 15000)
                        bonus = random.randint(0, 3000)
                        writer.writerow([payroll_id, emp_id, month, base_salary, bonus])
                    
                    i += 1
                
                # Check file size after batch
                current_size = os.path.getsize(output_file)
                if current_size >= target_size_bytes:
                    # Target size reached, stop generation
                    break
                
                # Progress reporting
                print(f"Generated {record_count:,} payroll records... ({current_size / (1024**3):.2f} GB)", file=sys.stderr)
        else:
            # Fixed record count mode
            for i in range(1, num_payroll_records + 1):
                payroll_id = f"PR{i:09d}"
                record_count += 1
                
                # Determine if this should be a bad record
                is_bad_record = random.random() < bad_record_rate
                
                if is_bad_record:
                    bad_record_count += 1
                    # Randomly choose type of corruption
                    bad_type = random.choice(['missing_field', 'invalid_salary', 'negative_value', 'unknown_emp_id'])
                    
                    if bad_type == 'missing_field':
                        month = random.choice(months)
                        base_salary = random.randint(3000, 15000)
                        bonus = random.randint(0, 3000)
                        writer.writerow([payroll_id, '', month, base_salary, bonus])
                        
                    elif bad_type == 'invalid_salary':
                        emp_id = f"EMP{random.randint(1, num_employees):06d}"
                        month = random.choice(months)
                        writer.writerow([payroll_id, emp_id, month, 'ABC', random.randint(0, 3000)])
                        
                    elif bad_type == 'negative_value':
                        emp_id = f"EMP{random.randint(1, num_employees):06d}"
                        month = random.choice(months)
                        base_salary = random.randint(3000, 15000)
                        bonus = -random.randint(100, 1000)
                        writer.writerow([payroll_id, emp_id, month, base_salary, bonus])
                        
                    else:  # unknown_emp_id
                        emp_id = f"EMP{random.randint(num_employees + 1, num_employees + 100000):06d}"
                        month = random.choice(months)
                        base_salary = random.randint(3000, 15000)
                        bonus = random.randint(0, 3000)
                        writer.writerow([payroll_id, emp_id, month, base_salary, bonus])
                else:
                    emp_id = f"EMP{random.randint(1, num_employees):06d}"
                    month = random.choice(months)
                    base_salary = random.randint(3000, 15000)
                    bonus = random.randint(0, 3000)
                    writer.writerow([payroll_id, emp_id, month, base_salary, bonus])
                
                # Progress reporting
                if i % 100000 == 0:
                    print(f"Generated {i:,} payroll records...", file=sys.stderr)
    
    return record_count, bad_record_count


def get_file_size_mb(file_path):
    """Get file size in MB."""
    return os.path.getsize(file_path) / (1024 * 1024)


def get_file_size_gb(file_path):
    """Get file size in GB."""
    return os.path.getsize(file_path) / (1024 * 1024 * 1024)


def main():
    parser = argparse.ArgumentParser(
        description='Generate synthetic data for Hadoop MapReduce Employee Payroll Join project'
    )
    parser.add_argument(
        '--employees',
        type=int,
        default=50000,
        help='Number of employees to generate (default: 50000)'
    )
    parser.add_argument(
        '--payroll-records',
        type=int,
        default=5000000,
        help='Number of payroll records to generate (default: 5000000)'
    )
    parser.add_argument(
        '--target-size-gb',
        type=float,
        default=None,
        help='Target payroll file size in GB (overrides --payroll-records if provided)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        required=True,
        help='Output directory for CSV files'
    )
    parser.add_argument(
        '--bad-record-rate',
        type=float,
        default=0.01,
        help='Percentage of corrupted records, 0.0-1.0 (default: 0.01)'
    )
    parser.add_argument(
        '--seed',
        type=int,
        default=None,
        help='Random seed for reproducibility (optional)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.employees <= 0:
        print("Error: --employees must be greater than 0", file=sys.stderr)
        sys.exit(1)
    
    if args.target_size_gb is None and args.payroll_records <= 0:
        print("Error: --payroll-records must be greater than 0", file=sys.stderr)
        sys.exit(1)
    
    if args.target_size_gb is not None and args.target_size_gb <= 0:
        print("Error: --target-size-gb must be greater than 0", file=sys.stderr)
        sys.exit(1)
    
    if not (0.0 <= args.bad_record_rate <= 1.0):
        print("Error: --bad-record-rate must be between 0.0 and 1.0", file=sys.stderr)
        sys.exit(1)
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    employees_file = os.path.join(args.output_dir, 'employees.csv')
    payroll_file = os.path.join(args.output_dir, 'payroll.csv')
    
    # Print configuration
    print(f"Starting data generation...")
    print(f"  Employees: {args.employees:,}")
    if args.target_size_gb is not None:
        print(f"  Payroll target size: {args.target_size_gb:.2f} GB")
    else:
        print(f"  Payroll records: {args.payroll_records:,}")
    print(f"  Bad record rate: {args.bad_record_rate * 100:.2f}%")
    print(f"  Output directory: {args.output_dir}")
    if args.seed is not None:
        print(f"  Random seed: {args.seed}")
    print()
    
    # Generate employees
    print("Generating employee records...")
    num_employees = generate_employees(args.employees, employees_file, args.seed)
    emp_size_mb = get_file_size_mb(employees_file)
    print(f"✓ Generated {num_employees:,} employee records")
    print(f"  File: {employees_file}")
    print(f"  Size: {emp_size_mb:.2f} MB")
    print()
    
    # Generate payroll
    print("Generating payroll records...")
    num_payroll, num_bad = generate_payroll(
        args.payroll_records,
        args.employees,
        payroll_file,
        args.bad_record_rate,
        args.seed,
        args.target_size_gb
    )
    
    payroll_size_mb = get_file_size_mb(payroll_file)
    payroll_size_gb = payroll_size_mb / 1024
    
    print(f"✓ Generated {num_payroll:,} payroll records")
    print(f"  File: {payroll_file}")
    if payroll_size_gb >= 1:
        print(f"  Size: {payroll_size_gb:.2f} GB")
    else:
        print(f"  Size: {payroll_size_mb:.2f} MB")
    print()
    
    # Print summary statistics
    print("=" * 70)
    print("SUMMARY STATISTICS")
    print("=" * 70)
    print(f"Total employees: {num_employees:,}")
    print(f"Total payroll records: {num_payroll:,}")
    print(f"Bad records injected: {num_bad:,} ({(num_bad/num_payroll)*100:.2f}%)")
    print(f"Good records: {num_payroll - num_bad:,} ({((num_payroll-num_bad)/num_payroll)*100:.2f}%)")
    print()
    print(f"Employees file size: {emp_size_mb:.2f} MB")
    if payroll_size_gb >= 1:
        print(f"Payroll file size: {payroll_size_gb:.2f} GB")
    else:
        print(f"Payroll file size: {payroll_size_mb:.2f} MB")
    
    total_size_mb = emp_size_mb + payroll_size_mb
    total_size_gb = total_size_mb / 1024
    if total_size_gb >= 1:
        print(f"Total size: {total_size_gb:.2f} GB")
    else:
        print(f"Total size: {total_size_mb:.2f} MB")
    print("=" * 70)
    print()
    print("Data generation completed successfully!")


if __name__ == '__main__':
    main()
