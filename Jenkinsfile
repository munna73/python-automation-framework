pipeline {
    agent any
    
    environment {
        ORACLE_PWD = credentials('oracle-password-id')
        POSTGRES_PWD = credentials('postgres-password-id')
        API_TOKEN = credentials('api-token-id')
        MQ_PWD = credentials('mq-password-id')
    }
    
    stages {
        stage('Setup') {
            steps {
                sh """
                    python -m venv venv
                    source venv/bin/activate
                    pip install --upgrade pip
                    pip install -r requirements.txt
                    mkdir -p logs output
                """
            }
        }
        
        stage('Code Quality') {
            steps {
                sh """
                    source venv/bin/activate
                    black --check .
                    flake8 .
                    isort --check-only .
                """
            }
        }
        
        stage('Unit Tests') {
            steps {
                sh """
                    source venv/bin/activate
                    pytest tests/ --html=output/unit_report.html --cov=. --cov-report=html
                """
            }
        }
        
        stage('BDD Tests') {
            steps {
                sh """
                    source venv/bin/activate
                    behave --junit --junit-directory output/junit --tags=@smoke
                """
            }
        }
        
        stage('Regression Tests') {
            when {
                anyOf {
                    branch 'main'
                    branch 'develop'
                }
            }
            steps {
                sh """
                    source venv/bin/activate
                    behave --junit --junit-directory output/junit --tags=@regression
                """
            }
        }
    }
    
    post {
        always {
            publishHTML([
                allowMissing: false,
                alwaysLinkToLastBuild: true,
                keepAll: true,
                reportDir: 'output',
                reportFiles: '*.html',
                reportName: 'Test Reports'
            ])
            
            junit 'output/junit/*.xml'
            
            archiveArtifacts artifacts: 'logs/**/*.log,output/**/*', fingerprint: true
        }
        
        failure {
            emailext (
                subject: "Build Failed: ${env.JOB_NAME} - ${env.BUILD_NUMBER}",
                body: "Build failed. Check console output at ${env.BUILD_URL}",
                to: "${env.CHANGE_AUTHOR_EMAIL}"
            )
        }
    }
}