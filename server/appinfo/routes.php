<?php

declare(strict_types=1);

return [
    'routes' => [
        // Block map retrieval (supports both query parameter /api/blockmap?path=... and path parameter /api/blockmap/{path})
        ['name' => 'delta#getBlockMapQuery', 'url' => '/api/blockmap', 'verb' => 'GET'],
        ['name' => 'delta#getBlockMap', 'url' => '/api/blockmap/{path}', 'verb' => 'GET',
         'requirements' => ['path' => '.+']],

        // Block-level write (supports both POST and PUT)
        ['name' => 'delta#putBlockQuery', 'url' => '/api/blocks', 'verb' => 'POST'],
        ['name' => 'delta#putBlockQueryPut', 'url' => '/api/blocks', 'verb' => 'PUT'],
        ['name' => 'delta#putBlock', 'url' => '/api/blocks/{path}', 'verb' => 'POST',
         'requirements' => ['path' => '.+']],
        ['name' => 'delta#putBlockPut', 'url' => '/api/blocks/{path}', 'verb' => 'PUT',
         'requirements' => ['path' => '.+']],

        // Finalize after block writes (supports both POST and PUT)
        ['name' => 'delta#finalizeQuery', 'url' => '/api/finalize', 'verb' => 'POST'],
        ['name' => 'delta#finalizeQueryPut', 'url' => '/api/finalize', 'verb' => 'PUT'],
        ['name' => 'delta#finalize', 'url' => '/api/finalize/{path}', 'verb' => 'POST',
         'requirements' => ['path' => '.+']],
        ['name' => 'delta#finalizePut', 'url' => '/api/finalize/{path}', 'verb' => 'PUT',
         'requirements' => ['path' => '.+']],

        // Status check
        ['name' => 'delta#status', 'url' => '/api/status', 'verb' => 'GET'],
    ],
];
