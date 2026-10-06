function ErrorState({ message }) {
  return (
    <div className="state-card error-card">
      <h2>Unable to load dashboard</h2>

      <p>{message}</p>

      <p>
        Check that the FastAPI server is running and
        that the API base URL is configured correctly.
      </p>
    </div>
  );
}

export default ErrorState;