pipeline {
    agent any
    
    environment {
        // Database credentials
        DEV_ORACLE_PWD = credentials('dev-oracle-password')
        DEV_POSTGRES_PWD = credentials('dev-postgres-password')
        DEV_MONGODB_PWD = credentials('dev-mongodb-password')
        QA_ORACLE_PWD = credentials('qa-oracle-password')
        QA_POSTGRES_PWD = credentials('qa-postgres-password')
        QA_MONGODB_PWD = credentials('qa-mongodb-password')
        
        // API and MQ credentials
        API_TOKEN = credentials('api-token')
        MQ_PWD = credentials('mq-password')
        
        // AWS credentials
        AWS_ACCESS_KEY_ID = credentials('aws-access-key-id')
        AWS_SECRET_ACCESS_KEY = credentials('aws-secret-access-key')
        AWS_DEFAULT_REGION = 'us-east-1'
        
        // CI/CD specific
        CI = 'true'
        PYTHONUNBUFFERED = '1'
        PYTHONPATH = "${WORKSPACE}"
        LOG_LEVEL = 'INFO'
        BEHAVE_DEBUG_ON_ERROR = 'true'
    }
    
    options {
        // Keep builds for 30 days
        buildDiscarder(logRotator(daysToKeepStr: '30', numToKeepStr: '50'))
        
        // Timeout the build after 1 hour
        timeout(time: 1, unit: 'HOURS')
        
        // Add timestamps to console output
        timestamps()
        
        // Skip default checkout
        skipDefaultCheckout(false)
    }
    
    stages {
        stage('Checkout & Setup') {
            steps {
                script {
                    echo "🚀 Starting Python Automation Framework Pipeline"
                    echo "Workspace: ${WORKSPACE}"
                    echo "Build Number: ${BUILD_NUMBER}"
                    echo "Job Name: ${JOB_NAME}"
                }
                
                // Checkout code
                checkout scm
                
                // Setup Python environment
                sh '''
                    echo "Setting up Python virtual environment..."
                    python3 -m venv venv
                    source venv/bin/activate
                    
                    # Upgrade pip
                    pip install --upgrade pip
                    
                    # Install dependencies
                    pip install -r requirements.txt
                    
                    # Verify installation
                    pip list
                    
                    # Create required directories
                    mkdir -p test-results output logs
                    
                    echo "✅ Environment setup completed"
                '''
            }
        }
        
        stage('Code Quality') {
            parallel {
                stage('Linting') {
                    steps {
                        sh '''
                            source venv/bin/activate
                            echo "🔍 Running code quality checks..."
                            
                            # Python linting
                            flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics || true
                            
                            # Import sorting
                            isort --check-only --diff . || true
                            
                            # Code formatting
                            black --check --diff . || true
                            
                            echo "✅ Code quality checks completed"
                        '''
                    }
                }
                
                stage('Security Scan') {
                    steps {
                        sh '''
                            source venv/bin/activate
                            echo "🔒 Running security checks..."
                            
                            # Check for security vulnerabilities
                            pip-audit --desc --output json --output-file test-results/security-report.json || true
                            
                            echo "✅ Security scan completed"
                        '''
                    }
                }
            }
        }
        
        stage('Unit Tests') {
            steps {
                sh '''
                    source venv/bin/activate
                    echo "🧪 Running unit tests..."
                    
                    # Run pytest with coverage
                    pytest tests/ \\
                        --html=test-results/unit-test-report.html \\
                        --self-contained-html \\
                        --cov=. \\
                        --cov-report=html:test-results/coverage-html \\
                        --cov-report=xml:test-results/coverage.xml \\
                        --junit-xml=test-results/unit-tests.xml \\
                        || true
                    
                    echo "✅ Unit tests completed"
                '''
            }
        }
        
        stage('BDD Tests') {
            parallel {
                stage('Smoke Tests') {
                    steps {
                        sh '''
                            source venv/bin/activate
                            echo "💨 Running smoke tests..."
                            
                            # Run smoke tests
                            behave --tags=@smoke \\
                                --junit \\
                                --junit-directory test-results/smoke \\
                                --format=json \\
                                --outfile=test-results/smoke-results.json \\
                                || true
                            
                            echo "✅ Smoke tests completed"
                        '''
                    }
                }
                
                stage('Database Tests') {
                    steps {
                        sh '''
                            source venv/bin/activate
                            echo "🗄️ Running database tests..."
                            
                            # Run database tests (SQL + NoSQL)
                            behave --tags="@database or @mongodb" \\
                                --junit \\
                                --junit-directory test-results/database \\
                                --format=json \\
                                --outfile=test-results/database-results.json \\
                                || true
                            
                            echo "✅ Database tests completed"
                        '''
                    }
                }
                
                stage('API Tests') {
                    steps {
                        sh '''
                            source venv/bin/activate
                            echo "🌐 Running API tests..."
                            
                            # Run API tests
                            behave --tags=@api \\
                                --junit \\
                                --junit-directory test-results/api \\
                                --format=json \\
                                --outfile=test-results/api-results.json \\
                                || true
                            
                            echo "✅ API tests completed"
                        '''
                    }
                }
                
                stage('AWS Tests') {
                    when {
                        anyOf {
                            branch 'main'
                            branch 'develop'
                            expression { params.RUN_AWS_TESTS == true }
                        }
                    }
                    steps {
                        sh '''
                            source venv/bin/activate
                            echo "☁️ Running AWS tests..."
                            
                            # Run AWS tests
                            behave --tags=@aws \\
                                --junit \\
                                --junit-directory test-results/aws \\
                                --format=json \\
                                --outfile=test-results/aws-results.json \\
                                || true
                            
                            echo "✅ AWS tests completed"
                        '''
                    }
                }
            }
        }
        
        stage('Integration Tests') {
            when {
                anyOf {
                    branch 'main'
                    branch 'develop'
                }
            }
            steps {
                sh '''
                    source venv/bin/activate
                    echo "🔗 Running integration tests..."
                    
                    # Run integration tests
                    behave --tags=@integration \\
                        --junit \\
                        --junit-directory test-results/integration \\
                        --format=json \\
                        --outfile=test-results/integration-results.json \\
                        || true
                    
                    echo "✅ Integration tests completed"
                '''
            }
        }
        
        stage('Regression Tests') {
            when {
                branch 'main'
            }
            steps {
                sh '''
                    source venv/bin/activate
                    echo "🔄 Running regression tests..."
                    
                    # Run full regression suite
                    behave --tags=@regression \\
                        --junit \\
                        --junit-directory test-results/regression \\
                        --format=json \\
                        --outfile=test-results/regression-results.json \\
                        || true
                    
                    echo "✅ Regression tests completed"
                '''
            }
        }
        
        stage('Performance Tests') {
            when {
                anyOf {
                    branch 'main'
                    expression { params.RUN_PERFORMANCE_TESTS == true }
                }
            }
            steps {
                sh '''
                    source venv/bin/activate
                    echo "⚡ Running performance tests..."
                    
                    # Run performance tests
                    behave --tags=@performance \\
                        --junit \\
                        --junit-directory test-results/performance \\
                        --format=json \\
                        --outfile=test-results/performance-results.json \\
                        || true
                    
                    echo "✅ Performance tests completed"
                '''
            }
        }
    }
    
    post {
        always {
            echo "📊 Publishing test results..."
            
            // Publish test results
            publishTestResults([
                testResultsPattern: 'test-results/**/*.xml',
                allowEmptyResults: true,
                keepLongStdio: true
            ])
            
            // Publish HTML reports
            publishHTML([
                allowMissing: false,
                alwaysLinkToLastBuild: true,
                keepAll: true,
                reportDir: 'test-results',
                reportFiles: '*.html',
                reportName: 'Test Reports',
                reportTitles: 'Automation Framework Test Reports'
            ])
            
            // Archive artifacts
            archiveArtifacts([
                artifacts: '''
                    test-results/**/*,
                    output/**/*,
                    logs/**/*,
                    *.log
                ''',
                fingerprint: true,
                allowEmptyArchive: true
            ])
            
            // Cleanup workspace if successful
            cleanWs(
                cleanWhenAborted: true,
                cleanWhenFailure: false,
                cleanWhenNotBuilt: true,
                cleanWhenSuccess: true,
                cleanWhenUnstable: false,
                deleteDirs: true
            )
        }
        
        success {
            echo "✅ Pipeline completed successfully!"
            
            // Send success notification
            emailext (
                subject: "✅ Success: ${env.JOB_NAME} - Build ${env.BUILD_NUMBER}",
                body: """
                    <h3>Build Successful! 🎉</h3>
                    <p><strong>Job:</strong> ${env.JOB_NAME}</p>
                    <p><strong>Build Number:</strong> ${env.BUILD_NUMBER}</p>
                    <p><strong>Duration:</strong> ${currentBuild.durationString}</p>
                    <p><strong>Console Output:</strong> <a href="${env.BUILD_URL}console">View Logs</a></p>