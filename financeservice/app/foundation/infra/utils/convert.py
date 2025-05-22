from typing import Union, Optional

def convert_amount(amount: Optional[Union[str, float, int]]) -> float:
    """
    금액 문자열을 float 타입으로 변환합니다.
    
    Args:
        amount: 변환할 금액 (문자열, float, int 또는 None)
        
    Returns:
        float: 변환된 금액. 입력이 None이거나 빈 문자열인 경우 0.0 반환
        
    Examples:
        >>> convert_amount("1,234,567")
        1234567.0
        >>> convert_amount("1,234,567.89")
        1234567.89
        >>> convert_amount(None)
        0.0
        >>> convert_amount("")
        0.0
    """
    if amount is None:
        return 0.0
        
    if isinstance(amount, (float, int)):
        return float(amount)
        
    # 문자열인 경우
    cleaned = str(amount).replace(",", "").strip()
    if not cleaned:
        return 0.0
        
    try:
        return float(cleaned)
    except ValueError:
        return 0.0 