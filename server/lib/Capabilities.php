<?php

declare(strict_types=1);

namespace OCA\CrispCloudDelta;

use OCP\Capabilities\ICapability;

class Capabilities implements ICapability {
    public function getCapabilities(): array {
        return [
            'crispcloud_delta' => [
                'enabled' => true,
                'version' => '0.3.0',
                'supportedAlgorithms' => ['fastcdc', 'fixed'],
                'defaultAlgorithm' => 'fastcdc',
                'blockSize' => 4 * 1024 * 1024,
                'minChunkSize' => Service\FastCdc::DEFAULT_MIN_SIZE,
                'avgChunkSize' => Service\FastCdc::DEFAULT_AVG_SIZE,
                'maxChunkSize' => Service\FastCdc::DEFAULT_MAX_SIZE,
            ],
        ];
    }
}
