pragma solidity ^0.8.20;

contract ISTCoin {
  string public constant name = "IST Coin";
  string public constant symbol = "IST";
  uint8 public constant decimals = 2;
  uint256 public constant totalSupply = 100_000_000 * 10**2; // TODO: check this

  // balances[address] = token balance of that address
  mapping(address => uint256) private balances;

  // allowances[owner][spender] = how much the spender is allowed to spend
  mapping(address => mapping(address => uint256)) private allowances;

  // Event emitted whenever tokens are transferred.
  event Transfer(address indexed from, address indexed to, uint256 value);

  // Event emitted whenever an allowance is created or updated.
  event Approval(address indexed owner, address indexed spender, uint256 value);

  constructor(address initialOwner) {        
    require(initialOwner != address(0), "initial owner cannot be zero address");
    
    balances[initialOwner] = totalSupply;
    emit Transfer(address(0), initialOwner, totalSupply);
  }

  // Returns the token balance of an account.
  function balanceOf(address account) external view returns (uint256) {
    return balances[account];
  }

  // Returns the current allowance from owner to spender.
  function allowance(address owner, address spender) external view returns (uint256) {
    return allowances[owner][spender];
  }

  // Approves another account (spender) to spend tokens on your behalf later using transferFrom.
  function approve(address spender, uint256 value) external returns (bool) {
    uint256 current = allowances[msg.sender][spender];

    // Protection against approval frontrunning.
    require(
        current == 0 || value == 0,
        "reset allowance to 0 first"
    );

    allowances[msg.sender][spender] = value;
    emit Approval(msg.sender, spender, value);
    return true;
  }

  // Transfers tokens from the account that called this function to another address.
  function transfer(address to, uint256 value) external returns (bool) {
    _transfer(msg.sender, to, value);
    return true;
  }

  // Allows someone to transfer tokens from another address, as long as they have enough allowance.
  function transferFrom(address from, address to, uint256 value) external returns (bool) {
    uint256 current = allowances[from][msg.sender];

    require(current >= value, "insufficient allowance");

    allowances[from][msg.sender] = current - value;
    emit Approval(from, msg.sender, allowances[from][msg.sender]);
    _transfer(from, to, value);
    return true;
  }

  function _transfer(address from, address to, uint256 value) internal {
    require(to != address(0), "invalid recipient");
    require(balances[from] >= value, "insufficient balance");

    balances[from] -= value;
    balances[to] += value;
    emit Transfer(from, to, value);
  }
}