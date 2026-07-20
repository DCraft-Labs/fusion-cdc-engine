import React from "react";
import { useLocation } from "react-router-dom";

interface Props {
  children: React.ReactNode;
}

interface State {
  hasError: boolean;
  error: Error | null;
}

class ErrorBoundaryInner extends React.Component<Props & { locationKey: string }, State> {
  constructor(props: Props & { locationKey: string }) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error };
  }

  componentDidUpdate(prevProps: Props & { locationKey: string }) {
    // Reset error state when route changes (back/forward navigation)
    if (prevProps.locationKey !== this.props.locationKey && this.state.hasError) {
      this.setState({ hasError: false, error: null });
    }
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="flex flex-col items-center justify-center h-64 gap-4">
          <p className="text-lg font-medium text-destructive">Something went wrong</p>
          <p className="text-sm text-muted-foreground">{this.state.error?.message}</p>
          <button
            className="px-4 py-2 text-sm font-medium rounded-md bg-primary text-primary-foreground hover:bg-primary/90"
            onClick={() => this.setState({ hasError: false, error: null })}
          >
            Try Again
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}

export function ErrorBoundary({ children }: Props) {
  const location = useLocation();
  return (
    <ErrorBoundaryInner locationKey={location.key}>
      {children}
    </ErrorBoundaryInner>
  );
}
