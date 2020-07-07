<?php

/**
 * MvcCore
 *
 * This source file is subject to the BSD 3 License
 * For the full copyright and license information, please view
 * the LICENSE.md file that are distributed with this source code.
 *
 * @copyright	Copyright (c) 2016 Tom Flídr (https://github.com/mvccore/mvccore)
 * @license  https://mvccore.github.io/docs/mvccore/5.0.0/LICENCE.md
 */

namespace MvcCore\Ext\Caches;

class Redis implements \MvcCore\Ext\ICache
{
	/** @var array */
	protected static $instances = [];

	/** @var array */
	protected static $defaults = [
		\MvcCore\Ext\ICache::CONNECTION_NAME		=> 'default',
		\MvcCore\Ext\ICache::CONNECTION_HOST		=> '127.0.0.1',
		\MvcCore\Ext\ICache::CONNECTION_PORT		=> 6379,
		\MvcCore\Ext\ICache::CONNECTION_DATABASE	=> NULL,
		\MvcCore\Ext\ICache::CONNECTION_TIMEOUT		=> NULL,
	];

	/** @var \stdClass|NULL */
	protected $config = NULL;

	/** @var \Redis|NULL */
	protected $redis = NULL;

	/** @var bool */
	protected $enabled = TRUE;

	/** @var \MvcCore\Application */
	protected $application = TRUE;

	/**
	 * @param string|array|NULL $connectionArguments...
	 * If string, it's used as connection name.
	 * If array, it's used as connection config array with keys:
	 *  - `name`		default: 'default'
	 *  - `host`		default: '127.0.0.1'
	 *  - `port`		default: 6379
	 *  - `database`	default: $_SERVER['SERVER_NAME']
	 *  - `timeout`		default: NULL
	 *  If NULL, there is returned `default` connection
	 *  name with default initial configuration values.
	 * @return \MvcCore\Ext\Caches\Redis|\MvcCore\Ext\ICache
	 */
	public static function GetInstance (/*...$connectionNameOrArguments = NULL*/) {
		$args = func_get_args();
		$nameKey = \MvcCore\Ext\ICache::CONNECTION_NAME;
		$config = static::$defaults;
		$connectionName = $config[$nameKey];
		if (isset($args[0])) {
			$arg = & $args[0];
			if (is_string($arg)) {
				$connectionName = $arg;
			} else if (is_array($arg)) {
				$connectionName = isset($arg[$nameKey])
					? $arg[$nameKey]
					: static::$defaults[$nameKey];
				$config = $arg;
			} else if ($arg !== NULL) {
				throw new \InvalidArgumentException(
					"[".get_class()."] Cache instance getter argument could be ".
					"only a string connection name or connection config array."
				);
			}
		}
		if (!isset(self::$instances[$connectionName]))
			self::$instances[$connectionName] = new static($config);
		return self::$instances[$connectionName];
	}

	/**
	 * @param array $config Connection config array with keys:
	 *  - `name`		default: 'default'
	 *  - `host`		default: '127.0.0.1'
	 *  - `port`		default: 6379
	 *  - `database`	default: $_SERVER['SERVER_NAME']
	 *  - `timeout`		default: NULL
	 */
	protected function __construct (array $config = []) {
		$hostKey	= \MvcCore\Ext\ICache::CONNECTION_HOST;
		$portKey	= \MvcCore\Ext\ICache::CONNECTION_PORT;
		$timeoutKey	= \MvcCore\Ext\ICache::CONNECTION_TIMEOUT;
		$dbKey		= \MvcCore\Ext\ICache::CONNECTION_DATABASE;

		$connectionArguments = [];

		if (!isset($config[$hostKey]))
			$config[$hostKey] = static::$defaults[$hostKey];
		$connectionArguments[] = $config[$hostKey];

		if (!isset($config[$portKey]))
			$config[$portKey] = static::$defaults[$portKey];
		$connectionArguments[] = $config[$portKey];

		if (isset($config[$timeoutKey])) {
			$connectionArguments[] = $config[$timeoutKey];
		} else if (static::$defaults[$timeoutKey] !== NULL) {
			$config[$timeoutKey] = static::$defaults[$timeoutKey];
			$connectionArguments[] = $config[$timeoutKey];
		}

		if (!isset($config[$dbKey]))
			$config[$dbKey]	= static::$defaults[$dbKey];

		$this->config = (object) $config;
		$this->application = \MvcCore\Application::GetInstance();
		$toolClass = $this->application->GetToolClass();
		$debugClass = $this->application->GetDebugClass();

		if (!class_exists('\Redis')) {
			$this->enabled = FALSE;
		} else {
			try {
				$this->redis = new \Redis();

				$connected = $toolClass::Invoke(
					[$this->redis, 'connect'],
					$connectionArguments,
					function ($errMsg, $errLevel, $errLine, $errContext) use (& $connected) {
						$connected = FALSE;
					}
				);
				$this->enabled = $connected;
				if ($connected)
					$this->redis->setOption(\Redis::OPT_PREFIX, $config[$dbKey].':');
			} catch (\Exception $e) {
				$debugClass::Log($e);
				$this->enabled = FALSE;
			}
		}
	}

	/**
	 * Get resource instance.
	 * @return \Redis|NULL
	 */
	public function GetResource () {
		return $this->redis;
	}

	/**
	 * Set resource instance.
	 * @param \Redis $resource
	 * @return \MvcCore\Ext\Caches\Redis|\MvcCore\Ext\ICache
	 */
	public function SetResource ($resource) {
		$this->redis = $resource;
		return $this;
	}

	/**
	 * Return initial configuration data.
	 * @return \stdClass
	 */
	public function GetConfig () {
		return $this->config;
	}

	/**
	 * Enable/disable cache component.
	 * @param bool $enable
	 * @return \MvcCore\Ext\Caches\Redis|\MvcCore\Ext\ICache
	 */
	public function SetEnabled ($enabled) {
		$this->enabled = $enabled;
		return $this;
	}

	/**
	 * Get if cache component is enabled/disabled.
	 * @return bool
	 */
	public function GetEnabled () {
		return $this->enabled;
	}

	/**
	 * Process given operations in transaction mode.
	 * @param array $ops Keys are redis functions names, values are functions arguments.
	 * @return array
	 */
	public function ProcessTransaction (array $ops = []) {
		$result = [];
		try {
			$multiRedis = $this->redis->multi();
			foreach ($ops as $oppName => $args)
				$multiRedis = $multiRedis->{$oppName}($args);
			$result = $multiRedis->exec();
		} catch (\Exception $e) {
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e;
			} else {
				$debugClass = $this->application->GetDebugClass();
				$debugClass::Log($e);
			}
		}
		return $result;
	}

	/**
	 * Set content under key with seconds expiration and tag(s).
	 * @param string $key
	 * @param mixed  $content
	 * @param int    $expirationSeconds
	 * @param array  $cacheTags
	 * @return bool
	 */
	public function Save ($key, $content, $expirationSeconds = NULL, $cacheTags = []) {
		$result = FALSE;
		if (!$this->enabled)
			return $result;
		try {
			if ($expirationSeconds === NULL) {
				$this->redis->set($key, serialize($content));
			} else {
				$this->redis->setEx($key, $expirationSeconds, serialize($content));
			}
			if ($cacheTags)
				foreach ($cacheTags as $tag)
					$this->redis->sAdd(self::TAG_PREFIX . $tag, $key);
			$result = TRUE;
		} catch (\Exception $e) {
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e;
			} else {
				$debugClass = $this->application->GetDebugClass();
				$debugClass::Log($e);
			}
		}
		return $result;
	}

	/**
	 * Set multiple contents under keys with seconds expirations and tags.
	 * @param array $keysAndContents
	 * @param int   $expirationSeconds
	 * @param array $cacheTags
	 * @return bool
	 */
	public function SaveMultiple ($keysAndContents, $expirationSeconds = NULL, $cacheTags = []) {
		$result = FALSE;
		if (!$this->enabled || $keysAndContents === NULL)
			return $result;
		try {
			$keysAndContents = array_map('serialize', $keysAndContents);
			if ($expirationSeconds === NULL) {
				$this->redis->mSet($keysAndContents);
			} else {
				foreach ($keysAndContents as $key => $serializedContent)
					$this->redis->setEx($key, $expirationSeconds, $serializedContent);
			}
			if ($cacheTags) {
				$args = array_keys($keysAndContents);
				if ($args !== NULL) {
					array_unshift($args, '');
					foreach ($cacheTags as $tag) {
						$args[0] = self::TAG_PREFIX . $tag;
						call_user_func_array([$this->redis, 'sAdd'], $args);
					}
				}
			}
			$result = TRUE;
		} catch (\Exception $e) {
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e;
			} else {
				$debugClass = $this->application->GetDebugClass();
				$debugClass::Log($e);
			}
		}
		return $result;
	}

	/**
	 * Return mixed content from cache by key or return `NULL` if content doens't
	 * exist in cache for given key.
	 * @param string        $key
	 * @param callable|NULL $notFoundCallback function ($cache, $cacheKey) { ... $cache->Save($cacheKey, $data); return $data; }
	 * @return mixed|NULL
	 */
	public function Load ($key, callable $notFoundCallback = NULL) {
		$result = NULL;
		$debugClass = $this->application->GetDebugClass();
		if (!$this->enabled) {
			if ($notFoundCallback !== NULL) {
				try {
					$result = call_user_func_array($notFoundCallback, [$this, $key]);
				} catch (\Exception $e1) {
					if ($this->application->GetEnvironment()->IsDevelopment()) {
						throw $e1;
					} else {
						$debugClass::Log($e1);
						$result = NULL;
					}
				}
			}
			return $result;
		}
		try {
			$rawContent = $this->redis->get($key);
			if ($rawContent !== FALSE) {
				$result = unserialize($rawContent);
			} else if ($notFoundCallback !== NULL) {
				$result = call_user_func_array($notFoundCallback, [$this, $key]);
			}
		} catch (\Exception $e2) {
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e2;
			} else {
				$debugClass::Log($e2);
				$result = NULL;
			}
		}
		return $result;
	}

	/**
	 * Get content by key.
	 * @param \string[]     $keys
	 * @param callable|NULL $notFoundCallback function ($cache, $cacheKey) { ... $cache->Save($cacheKey, $data); return $data; }
	 * @return mixed|NULL
	 */
	public function LoadMultiple (array $keys, callable $notFoundCallback = NULL) {
		$results = [];
		$keysArr = func_get_args();
		if (count($keysArr) === 1) {
			if (is_array($keys)) {
				$keysArr = $keys;
			} else if (is_string($keys)) {
				$keysArr = [$keys];
			}
		}
		$debugClass = $this->application->GetDebugClass();
		if (!$this->enabled) {
			if ($notFoundCallback !== NULL) {
				foreach ($keysArr as $index => $key) {
					try {
						$results[$index] = call_user_func_array(
							$notFoundCallback, [$this, $key]
						);
					} catch (\Exception $e1) {
						if ($this->application->GetEnvironment()->IsDevelopment()) {
							throw $e1;
						} else {
							$debugClass::Log($e1);
							$results[$index] = NULL;
						}
					}
				}
				return $results;
			} else {
				return NULL;
			}
		}
		try {
			$rawContents = $this->redis->mGet($keysArr);
		} catch (\Exception $e2) {
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e2;
			} else {
				$debugClass::Log($e2);
			}
		}
		foreach ($rawContents as $index => $rawContent) {
			try {
				if ($rawContent !== FALSE) {
					$results[$index] = unserialize($rawContent);
				} else if ($notFoundCallback !== NULL) {
					$results[$index] = call_user_func_array($notFoundCallback, [$this, $keys[$index]]);
				}
			} catch (\Exception $e3) {
				if ($this->application->GetEnvironment()->IsDevelopment()) {
					throw $e3;
				} else {
					$debugClass::Log($e3);
					$results[$index] = NULL;
				}
			}
		}
		return $results;
	}

	/**
	 * Delete cache record by key.
	 * @param string $key
	 * @return bool
	 */
	public function Delete ($key) {
		if (!$this->enabled) return FALSE;
		$deletedKeysCount = 0;
		try {
			$deletedKeysCount = $this->redis->del($key);
		} catch (\Exception $e) {
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e;
			} else {
				$debugClass = $this->application->GetDebugClass();
				$debugClass::Log($e);
			}
		}
		return $deletedKeysCount;
	}

	/**
	 * Delete cache record by key.
	 * @param \string[] $keys
	 * @param array $keysTags
	 * @return int
	 */
	public function DeleteMultiple (array $keys, array $keysTags = []) {
		if (!$this->enabled) return 0;
		$deletedKeysCount = 0;
		try {
			if (count($keys) > 0) {
				$deletedKeysCount = call_user_func_array(
					[$this->redis, 'del'],
					$keys
				);
			}
			if (count($keysTags) > 0) {
				$setsAndKeysToRemove = [];
				foreach ($keysTags as $key => $tags) {
					foreach ($tags as $tag) {
						$cacheTag = self::TAG_PREFIX . $tag;
						if (!isset($setsAndKeysToRemove[$cacheTag]))
							$setsAndKeysToRemove[$cacheTag] = [];
						$setsAndKeysToRemove[$cacheTag][] = $key;
					}
				}
				foreach ($setsAndKeysToRemove as $cacheTag => $keysToRemove) {
					if ($keysToRemove === NULL) continue;
					array_unshift($keysToRemove, $cacheTag);
					call_user_func_array(
						[$this->redis, 'sRem'],
						$keysToRemove
					);
				}
			}
		} catch (\Exception $e) {
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e;
			} else {
				$debugClass = $this->application->GetDebugClass();
				$debugClass::Log($e);
			}
		}
		return $deletedKeysCount;
	}

	/**
	 * Delete cache record by key.
	 * @param string|array $tags
	 * @return int
	 */
	public function DeleteByTags ($tags) {
		if (!$this->enabled) return 0;
		$tagsArr = func_get_args();
		if (count($tagsArr) === 1) {
			if (is_array($tags)) {
				$tagsArr = $tags;
			} else if (is_string($tags)) {
				$tagsArr = [$tags];
			}
		}
		$keysToDelete = [];
		foreach ($tagsArr as $tag) {
			$cacheTag = self::TAG_PREFIX . $tag;
			$keysToDelete[] = $cacheTag;
			$keysToDeleteLocal = $this->redis->sMembers($cacheTag);
			$keysToDelete = array_merge($keysToDelete, $keysToDeleteLocal);
		}
		$deletedKeysCount = 0;
		if (count($keysToDelete) > 0) {
			try {
				$deletedKeysCount = call_user_func_array(
					[$this->redis, 'del'],
					$keysToDelete
				);
			} catch (\Exception $e) {
				if ($this->application->GetEnvironment()->IsDevelopment()) {
					throw $e;
				} else {
					$debugClass = $this->application->GetDebugClass();
					$debugClass::Log($e);
				}
			}
		}
		return $deletedKeysCount;
	}

	/**
	 * Return `1` if cache has any record under given key, `0` if not.
	 * @param string $key
	 * @return int
	 */
	public function Has ($key) {
		$result = 0;
		if (!$this->enabled) return $result;
		try {
			$result = $this->redis->exists($key);
		} catch (\Exception $e) {
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e;
			} else {
				$debugClass = $this->application->GetDebugClass();
				$debugClass::Log($e);
			}
		}
		return $result;
	}

	/**
	 * Return number of records existing in cache under given keys, `0` if nothing.
	 * @param \string[] $key
	 * @return int
	 */
	public function HasMultiple ($keys) {
		$result = 0;
		if (!$this->enabled) return $result;
		$keysArr = func_get_args();
		if (count($keysArr) === 1) {
			if (is_array($keys)) {
				$keysArr = $keys;
			} else if (is_string($keys)) {
				$keysArr = [$keys];
			}
		}
		try {
			$result = call_user_func_array(
				[$this->redis, 'exists'],
				$keysArr
			);
		} catch (\Exception $e) {
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e;
			} else {
				$debugClass = $this->application->GetDebugClass();
				$debugClass::Log($e);
			}
		}
		return $result;
	}

	/**
	 * Remove everything from used cache database.
	 * @return bool
	 */
	public function Clear () {
		$result = FALSE;
		if (!$this->enabled) return $result;
		try {
			$result = $this->redis->flushDb();
		} catch (\Exception $e) {
			if ($this->application->GetEnvironment()->IsDevelopment()) {
				throw $e;
			} else {
				$debugClass = $this->application->GetDebugClass();
				$debugClass::Log($e);
			}
		}
		return $result;
	}
}