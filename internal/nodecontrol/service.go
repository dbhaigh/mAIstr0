package nodecontrol

import "context"

func RunService(options Options) error {
	return runAsService(func(ctx context.Context, serviceManaged bool) error {
		options.ServiceManaged = options.ServiceManaged || serviceManaged
		return Run(ctx, options)
	})
}
