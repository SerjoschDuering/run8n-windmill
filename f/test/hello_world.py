def main(name: str = "World") -> dict:
    """
    A simple hello world script to test the Windmill sync.
    
    Args:
        name: Name to greet (default: World)
    
    Returns:
        Greeting message
    """
    return {"message": f"Hello, {name}!"}
