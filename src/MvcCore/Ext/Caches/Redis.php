<?php

/**
 * MvcCore
 *
 * This source file is subject to the BSD 3 License
 * For the full copyright and license information, please view
 * the LICENSE.md file that are distributed with this source code.
 *
 * @copyright	Copyright (c) 2016 Tom Flidr (https://github.com/mvccore)
 * @license		https://mvccore.github.io/docs/mvccore/5.0.0/LICENSE.md
 */

namespace MvcCore\Ext\Caches;

/**
 * @method static \MvcCore\Ext\Caches\Redis GetInstance(string|array|NULL $connectionArguments,...) 
 * Create or get cached cache wrapper instance.
 * If first argument is string, it's used as connection name.
 * If first argument is array, it's used as connection config array with keys:
 *  - `name`     default: `default`,
 *  - `host`     default: `127.0.0.1`,
 *  - `port`     default: `6379`,
 *  - `database` default: `$_SERVER['SERVER_NAME']`,
 *  - `timeout`  default: `0.5` seconds,
 *  - `provider` default: `[...]`, provider specific configuration.
 *  If no argument provided, there is returned `default` 
 *  connection name with default initial configuration values.
 * @method \Redis|NULL GetProvider() Get `\Redis` provider instance.
 * @method \MvcCore\Ext\Caches\Redis SetProvider(\Redis|NULL $provider) Set `\Redis` provider instance.
 * @property \Redis|NULL $provider
 */
class		Redis
extends		\MvcCore\Ext\Caches\Base
implements	\MvcCore\Ext\ICache {
	
	/**
	 * MvcCore Extension - Cache - Redis - version:
	 * Comparison by PHP function version_compare();
	 * @see http://php.net/manual/en/function.version-compare.php
	 */
	const VERSION = '5.2.0';

	/** @var array */
	protected static $defaults	= [
		\MvcCore\Ext\ICache::CONNECTION_PERSISTENCE	=> 'default',
		\MvcCore\Ext\ICache::CONNECTION_NAME		=> NULL,
		\MvcCore\Ext\ICache::CONNECTION_HOST		=> '127.0.0.1',
		\MvcCore\Ext\ICache::CONNECTION_PORT		=> 6379,
		\MvcCore\Ext\ICache::CONNECTION_TIMEOUT		=> 0.5, // in seconds
		\MvcCore\Ext\ICache::PROVIDER_CONFIG		=> [
			'\Redis::OPT_SERIALIZER'				=> '\Redis::SERIALIZER_IGBINARY', // PHP serializer used if not available
			'\Redis::OPT_READ_TIMEOUT'				=> 0.01,  // in seconds
			'\Redis::OPT_MAX_RETRIES'				=> 5,
		]
	];

	/**
	 * @inheritDoc
	 * @param array $config Connection config array with keys:
	 *  - `name`     default: `default`,
	 *  - `host`     default: `127.0.0.1`,
	 *  - `port`     default: `6379`,
	 *  - `database` default: `$_SERVER['SERVER_NAME']`,
	 *  - `timeout`  default: `0.5` seconds,
	 *  - `provider` default: `[...]`, provider specific configuration.
	 */
	protected function __construct (array $config = []) {
		parent::__construct($config);
		$this->installed = class_exists('\Redis');
	}

	/**
	 * @inheritDoc
	 * @return bool
	 */
	public function Connect () {
		if ($this->connected) {
			return TRUE;
		} else if (!$this->installed) {
			$this->enabled = FALSE;
			$this->connected = FALSE;
		} else {
			try {
				$this->provider = new \Redis();
				if ($this->provider->isConnected()) {
					$this->connected = TRUE;
				} else {
					$this->connected = $this->connectExecute();
					$this->connectConfigure();
				}
				$this->enabled = $this->connected;
				if ($this->enabled) 
					$this->provider->setOption(
						\Redis::OPT_PREFIX, 
						$this->config->{self::CONNECTION_DATABASE}.':'
					);
			} catch (\Exception $e1) { // backward compatibility
				$this->exceptionHandler($e1);
				$this->connected = FALSE;
				$this->enabled = FALSE;
			} catch (\Throwable $e2) {
				$this->exceptionHandler($e2);
				$this->connected = FALSE;
				$this->enabled = FALSE;
			}
		}
		return $this->connected;
	}

	/**
	 * Process every request connection or first persistent connection.
	 * @return bool
	 */
	protected function connectExecute () {
		$toolClass	= $this->application->GetToolClass();
		$persKey	= self::CONNECTION_PERSISTENCE;
		$timeoutKey = self::CONNECTION_TIMEOUT;
		$connMethodName = 'connect';
		$connectionArguments = [
			$this->config->{self::CONNECTION_HOST},
			$this->config->{self::CONNECTION_PORT},
			isset($this->config->{$timeoutKey})
				? $this->config->{$timeoutKey}
				: static::$defaults[$timeoutKey]
		];
		if (isset($this->config->{$persKey})) {
			$connectionArguments[] = $this->config->{$persKey};
			$connMethodName = 'pconnect';
		}
		$connected = $toolClass::Invoke(
			[$this->provider, $connMethodName],
			$connectionArguments,
			function ($errMsg, $errLevel, $errLine, $errContext) use (& $connected) {
				$connected = FALSE;
			}
		);
		return !!$connected;
	}
	
	/**
	 * Configure connection provider after connection is established.
	 * @return void
	 */
	protected function connectConfigure () {
		$provKey = self::PROVIDER_CONFIG;
		$provConfig = isset($this->config->{$provKey})
			? $this->config->{$provKey}
			: [];
		$provConfigDefault = static::$defaults[$provKey];
		$redisConstBegin = '\Redis::';
		foreach ($provConfigDefault as $constStr => $rawValue) {
			$const = constant($constStr);
			if (!isset($provConfig[$const])) {
				if (is_string($rawValue) && strpos($rawValue, $redisConstBegin) === 0) {
					if (!defined($rawValue))
						continue;
					$value = constant($rawValue);
				} else {
					$value = $rawValue;
				}
				$provConfig[$const] = $value;
			}
		}
		if (!isset($provConfig[\Redis::OPT_SERIALIZER]))
			$provConfig[\Redis::OPT_SERIALIZER] = \Redis::SERIALIZER_PHP;
		foreach ($provConfig as $provOptKey => $provOptVal)
			$this->provider->setOption($provOptKey, $provOptVal);
	}
	
	/**
	 * Process given operations in transaction mode.
	 * @param  array $ops Keys are client functions names, values are functions arguments.
	 * @return array
	 */
	public function ProcessTransaction (array $ops = []) {
		$result = [];
		try {
			$multiRedis = $this->provider->multi();
			foreach ($ops as $oppName => $args)
				$multiRedis = $multiRedis->{$oppName}($args);
			$result = $multiRedis->exec();
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $result;
	}

	/**
	 * @inheritDoc
	 * @param  string   $key
	 * @param  mixed    $content
	 * @param  int|NULL $expirationSeconds
	 * @param  array    $cacheTags
	 * @return bool
	 */
	public function Save ($key, $content, $expirationSeconds = NULL, $cacheTags = []) {
		$result = FALSE;
		if (!$this->enabled)
			return $result;
		try {
			if ($expirationSeconds === NULL) {
				$this->provider->set($key, [$content]);
			} else {
				$this->provider->setEx($key, $expirationSeconds, [$content]);
			}
			if ($cacheTags)
				foreach ($cacheTags as $tag)
					$this->provider->sAdd(self::TAG_PREFIX . $tag, $key);
			$result = TRUE;
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $result;
	}

	/**
	 * @inheritDoc
	 * @param  array    $keysAndContents
	 * @param  int|NULL $expirationSeconds
	 * @param  array    $cacheTags
	 * @return bool
	 */
	public function SaveMultiple ($keysAndContents, $expirationSeconds = NULL, $cacheTags = []) {
		$result = FALSE;
		if (!$this->enabled || $keysAndContents === NULL)
			return $result;
		try {
			$keysAndContents = array_map(function ($item) {
				return [$item];
			}, $keysAndContents);
			if ($expirationSeconds === NULL) {
				$this->provider->mSet($keysAndContents);
			} else {
				foreach ($keysAndContents as $key => $content)
					$this->provider->setEx($key, $expirationSeconds, $content);
			}
			if (count($cacheTags) > 0) {
				$args = array_keys($keysAndContents);
				array_unshift($args, ''); // will be replaced with tag set key
				foreach ($cacheTags as $tag) {
					$args[0] = self::TAG_PREFIX . $tag;
					call_user_func_array([$this->memcached, 'sAdd'], $args);
				}
			}
			$result = TRUE;
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $result;
	}

	/**
	 * @inheritDoc
	 * @param  string        $key
	 * @param  callable|NULL $notFoundCallback function ($cache, $cacheKey) { ... $cache->Save($cacheKey, $data); return $data; }
	 * @return mixed|NULL
	 */
	public function Load ($key, callable $notFoundCallback = NULL) {
		$result = NULL;
		if (!$this->enabled) {
			if ($notFoundCallback !== NULL) {
				try {
					$result = call_user_func_array($notFoundCallback, [$this, $key]);
				} catch (\Exception $e1) { // backward compatibility
					$result = NULL;
					$this->exceptionHandler($e1);
				} catch (\Throwable $e2) {
					$result = NULL;
					$this->exceptionHandler($e2);
				}
			}
			return $result;
		}
		try {
			$rawArray = $this->provider->get($key);
			if ($rawArray !== FALSE) {
				$result = $rawArray[0];
			} else if ($notFoundCallback !== NULL) {
				$result = call_user_func_array($notFoundCallback, [$this, $key]);
			}
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $result;
	}

	/**
	 * @inheritDoc
	 * @param  \string[]     $keys
	 * @param  callable|NULL $notFoundCallback function ($cache, $cacheKey) { ... $cache->Save($cacheKey, $data); return $data; }
	 * @return array|NULL
	 */
	public function LoadMultiple (array $keys, callable $notFoundCallback = NULL) {
		$results = [];
		if (!$this->enabled) {
			if ($notFoundCallback !== NULL) {
				foreach ($keys as $index => $key) {
					try {
						$results[$index] = call_user_func_array(
							$notFoundCallback, [$this, $key]
						);
					} catch (\Exception $e1) { // backward compatibility
						$results[$index] = NULL;
						$this->exceptionHandler($e1);
					} catch (\Throwable $e2) {
						$results[$index] = NULL;
						$this->exceptionHandler($e2);
					}
				}
				return $results;
			} else {
				return NULL;
			}
		}
		try {
			$rawContents = $this->provider->mGet($keys);
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		foreach ($rawContents as $index => $rawArray) {
			try {
				if ($rawArray !== FALSE) {
					$results[$index] = $rawArray[0];
				} else if ($notFoundCallback !== NULL) {
					$results[$index] = call_user_func_array($notFoundCallback, [$this, $keys[$index]]);
				}
			} catch (\Exception $e1) { // backward compatibility
				$results[$index] = NULL;
				$this->exceptionHandler($e1);
			} catch (\Throwable $e2) {
				$results[$index] = NULL;
				$this->exceptionHandler($e2);
			}
		}
		return $results;
	}

	/**
	 * @inheritDoc
	 * @param  string $key
	 * @return bool
	 */
	public function Delete ($key) {
		if (!$this->enabled) return FALSE;
		$deletedKeysCount = 0;
		try {
			$deletedKeysCount = $this->provider->del($key);
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $deletedKeysCount === 1;
	}

	/**
	 * @inheritDoc
	 * @param  \string[] $keys
	 * @param  array     $keysTags
	 * @return int
	 */
	public function DeleteMultiple (array $keys, array $keysTags = []) {
		if (!$this->enabled) return 0;
		$deletedKeysCount = 0;
		try {
			if (count($keys) > 0) {
				$deletedKeysCount = call_user_func_array(
					[$this->provider, 'del'],
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
						[$this->provider, 'sRem'],
						$keysToRemove
					);
				}
			}
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $deletedKeysCount;
	}

	/**
	 * @inheritDoc
	 * @param  string|array $tags
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
			$keysToDelete[$cacheTag] = TRUE;
			$keys2DeleteLocal = $this->provider->sMembers($cacheTag);
			foreach ($keys2DeleteLocal as $key2DeleteLocal)
				$keysToDelete[$key2DeleteLocal] = TRUE;
		}
		$deletedKeysCount = 0;
		if (count($keysToDelete) > 0) {
			try {
				$deletedKeysCount = call_user_func_array(
					[$this->provider, 'del'],
					array_keys($keysToDelete)
				);
			} catch (\Exception $e1) { // backward compatibility
				$this->exceptionHandler($e1);
			} catch (\Throwable $e2) {
				$this->exceptionHandler($e2);
			}
		}
		return $deletedKeysCount;
	}

	/**
	 * @inheritDoc
	 * @param  string $key
	 * @return bool
	 */
	public function Has ($key) {
		$result = FALSE;
		if (!$this->enabled) return $result;
		try {
			$result = $this->provider->exists($key) === 1;
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $result;
	}

	/**
	 * @inheritDoc
	 * @param  string|\string[] $keys
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
				[$this->provider, 'exists'],
				$keysArr
			);
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $result;
	}

	/**
	 * @inheritDoc
	 * @return bool
	 */
	public function Clear () {
		$result = FALSE;
		if (!$this->enabled) return $result;
		try {
			$result = $this->provider->flushDb();
		} catch (\Exception $e1) { // backward compatibility
			$this->exceptionHandler($e1);
		} catch (\Throwable $e2) {
			$this->exceptionHandler($e2);
		}
		return $result;
	}
}