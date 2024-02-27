//go:build linux

/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package overlay

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/pkg/snapshotters"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/overlay/overlayutils"
	"github.com/containerd/containerd/snapshots/storage"
	"github.com/containerd/continuity/fs"
	"github.com/sirupsen/logrus"
)

// upperdirKey is a key of an optional label to each snapshot.
// This optional label of a snapshot contains the location of "upperdir" where
// the change set between this snapshot and its parent is stored.
const upperdirKey = "containerd.io/snapshot/overlay.upperdir"

// const activeSnapshotPath = "containerd.io/snapshot/overlay.active.path"

// SnapshotterConfig is used to configure the overlay snapshotter instance
type SnapshotterConfig struct {
	asyncRemove   bool
	upperdirLabel bool
	ms            MetaStore
	mountOptions  []string
}

// Opt is an option to configure the overlay snapshotter
type Opt func(config *SnapshotterConfig) error

// AsynchronousRemove defers removal of filesystem content until
// the Cleanup method is called. Removals will make the snapshot
// referred to by the key unavailable and make the key immediately
// available for re-use.
func AsynchronousRemove(config *SnapshotterConfig) error {
	config.asyncRemove = true
	return nil
}

// WithUpperdirLabel adds as an optional label
// "containerd.io/snapshot/overlay.upperdir". This stores the location
// of the upperdir that contains the changeset between the labelled
// snapshot and its parent.
func WithUpperdirLabel(config *SnapshotterConfig) error {
	config.upperdirLabel = true
	return nil
}

// WithMountOptions defines the default mount options used for the overlay mount.
// NOTE: Options are not applied to bind mounts.
func WithMountOptions(options []string) Opt {
	return func(config *SnapshotterConfig) error {
		config.mountOptions = append(config.mountOptions, options...)
		return nil
	}
}

type MetaStore interface {
	TransactionContext(ctx context.Context, writable bool) (context.Context, storage.Transactor, error)
	WithTransaction(ctx context.Context, writable bool, fn storage.TransactionCallback) error
	Close() error
}

// WithMetaStore allows the MetaStore to be created outside the snapshotter
// and passed in.
func WithMetaStore(ms MetaStore) Opt {
	return func(config *SnapshotterConfig) error {
		config.ms = ms
		return nil
	}
}

type snapshotter struct {
	root          string
	ms            MetaStore
	asyncRemove   bool
	upperdirLabel bool
	options       []string
}

// NewSnapshotter returns a Snapshotter which uses overlayfs. The overlayfs
// diffs are stored under the provided root. A metadata file is stored under
// the root.
func NewSnapshotter(root string, opts ...Opt) (snapshots.Snapshotter, error) {
	var config SnapshotterConfig
	for _, opt := range opts {
		if err := opt(&config); err != nil {
			return nil, err
		}
	}

	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	supportsDType, err := fs.SupportsDType(root)
	if err != nil {
		return nil, err
	}
	if !supportsDType {
		return nil, fmt.Errorf("%s does not support d_type. If the backing filesystem is xfs, please reformat with ftype=1 to enable d_type support", root)
	}
	if config.ms == nil {
		config.ms, err = storage.NewMetaStore(filepath.Join(root, "metadata.db"))
		if err != nil {
			return nil, err
		}
	}

	if err := os.Mkdir(filepath.Join(root, "snapshots"), 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}

	if !hasOption(config.mountOptions, "userxattr", false) {
		// figure out whether "userxattr" option is recognized by the kernel && needed
		userxattr, err := overlayutils.NeedsUserXAttr(root)
		if err != nil {
			logrus.WithError(err).Warnf("cannot detect whether \"userxattr\" option needs to be used, assuming to be %v", userxattr)
		}
		if userxattr {
			config.mountOptions = append(config.mountOptions, "userxattr")
		}
	}

	if !hasOption(config.mountOptions, "index", false) && supportsIndex() {
		config.mountOptions = append(config.mountOptions, "index=off")
	}

	return &snapshotter{
		root:          root,
		ms:            config.ms,
		asyncRemove:   config.asyncRemove,
		upperdirLabel: config.upperdirLabel,
		options:       config.mountOptions,
	}, nil
}

func hasOption(options []string, key string, hasValue bool) bool {
	for _, option := range options {
		if hasValue {
			if strings.HasPrefix(option, key) && len(option) > len(key) && option[len(key)] == '=' {
				return true
			}
		} else if option == key {
			return true
		}
	}
	return false
}

// Stat returns the info for an active or committed snapshot by name or
// key.
//
// Should be used for parent resolution, existence checks and to discern
// the kind of snapshot.
func (o *snapshotter) Stat(ctx context.Context, key string) (info snapshots.Info, err error) {
	var id string
	if err := o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		id, info, _, err = storage.GetInfo(ctx, key)
		return err
	}); err != nil {
		return info, err
	}

	if o.upperdirLabel {
		if info.Labels == nil {
			info.Labels = make(map[string]string)
		}
		info.Labels[upperdirKey] = o.upperPath(&info, id, key)
	}
	return info, nil
}

func (o *snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (newInfo snapshots.Info, err error) {
	err = o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		newInfo, err = storage.UpdateInfo(ctx, info, fieldpaths...)
		if err != nil {
			return err
		}

		if o.upperdirLabel {
			id, _, _, err := storage.GetInfo(ctx, newInfo.Name)
			if err != nil {
				return err
			}
			if newInfo.Labels == nil {
				newInfo.Labels = make(map[string]string)
			}
			newInfo.Labels[upperdirKey] = o.upperPath(&newInfo, id, newInfo.Name)
		}
		return nil
	})
	return newInfo, err
}

// Usage returns the resources taken by the snapshot identified by key.
//
// For active snapshots, this will scan the usage of the overlay "diff" (aka
// "upper") directory and may take some time.
//
// For committed snapshots, the value is returned from the metadata database.
func (o *snapshotter) Usage(ctx context.Context, key string) (_ snapshots.Usage, err error) {
	var (
		usage snapshots.Usage
		info  snapshots.Info
		id    string
	)
	if err := o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		id, info, usage, err = storage.GetInfo(ctx, key)
		return err
	}); err != nil {
		return usage, err
	}

	if info.Kind == snapshots.KindActive {
		upperPath := o.upperPath(&info, id, key)
		du, err := fs.DiskUsage(ctx, upperPath)
		if err != nil {
			// TODO(stevvooe): Consider not reporting an error in this case.
			return snapshots.Usage{}, err
		}
		usage = snapshots.Usage(du)
	}
	return usage, nil
}

func (o *snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return o.createSnapshot(ctx, snapshots.KindActive, key, parent, opts)
}

func (o *snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return o.createSnapshot(ctx, snapshots.KindView, key, parent, opts)
}

// Mounts returns the mounts for the transaction identified by key. Can be
// called on an read-write or readonly transaction.
//
// This can be used to recover mounts after calling View or Prepare.
func (o *snapshotter) Mounts(ctx context.Context, key string) (_ []mount.Mount, err error) {
	var s storage.Snapshot
	var info snapshots.Info
	if err := o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		s, err = storage.GetSnapshot(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to get active mount: %w", err)
		}

		_, info, _, err = storage.GetInfo(ctx, key)

		if err != nil {
			return fmt.Errorf("failed to get active mount: %w", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return o.mounts(&info, s, key), nil
}

func (o *snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	return o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		// grab the existing id
		id, info, _, err := storage.GetInfo(ctx, key)
		if err != nil {
			return err
		}

		usage, err := fs.DiskUsage(ctx, o.upperPath(&info, id, key))
		if err != nil {
			return err
		}

		if _, err = storage.CommitActive(ctx, key, name, snapshots.Usage(usage), opts...); err != nil {
			return fmt.Errorf("failed to commit snapshot %s: %w", key, err)
		}
		return nil
	})
}

// Remove abandons the snapshot identified by key. The snapshot will
// immediately become unavailable and unrecoverable. Disk space will
// be freed up on the next call to `Cleanup`.
func (o *snapshotter) Remove(ctx context.Context, key string) (err error) {
	var removals []string
	// Remove directories after the transaction is closed, failures must not
	// return error since the transaction is committed with the removal
	// key no longer available.
	defer func() {
		if err == nil {
			for _, dir := range removals {
				if err := os.RemoveAll(dir); err != nil {
					log.G(ctx).WithError(err).WithField("path", dir).Warn("failed to remove directory")
				}
			}
		}
	}()

	_, info, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		if strings.Contains(err.Error(), "snapshots does not exist") {
			return nil
		}

		return fmt.Errorf("failed to get snapshots info: %w", err)
	}

	var home string
	if home, err = getActivePath(&info, key); err == nil {
		if err = removeActivePath(&info, key); err != nil {
			return fmt.Errorf("failed to remove directory %v: %w", home, err)
		}
	}

	return o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		_, _, err = storage.Remove(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to remove snapshot %s: %w", key, err)
		}

		if !o.asyncRemove {
			removals, err = o.getCleanupDirectories(ctx)
			if err != nil {
				return fmt.Errorf("unable to get directories for removal: %w", err)
			}
		}
		return nil
	})
}

// Walk the snapshots.
func (o *snapshotter) Walk(ctx context.Context, fn snapshots.WalkFunc, fs ...string) error {
	return o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		if o.upperdirLabel {
			return storage.WalkInfo(ctx, func(ctx context.Context, info snapshots.Info) error {
				id, _, _, err := storage.GetInfo(ctx, info.Name)
				if err != nil {
					return err
				}
				if info.Labels == nil {
					info.Labels = make(map[string]string)
				}
				info.Labels[upperdirKey] = o.upperPath(&info, id, info.Name)
				return fn(ctx, info)
			}, fs...)
		}
		return storage.WalkInfo(ctx, fn, fs...)
	})
}

// Cleanup cleans up disk resources from removed or abandoned snapshots
func (o *snapshotter) Cleanup(ctx context.Context) error {
	cleanup, err := o.cleanupDirectories(ctx)
	if err != nil {
		return err
	}

	for _, dir := range cleanup {
		if err := os.RemoveAll(dir); err != nil {
			log.G(ctx).WithError(err).WithField("path", dir).Warn("failed to remove directory")
		}
	}

	return nil
}

func (o *snapshotter) cleanupDirectories(ctx context.Context) (_ []string, err error) {
	var cleanupDirs []string
	// Get a write transaction to ensure no other write transaction can be entered
	// while the cleanup is scanning.
	if err := o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		cleanupDirs, err = o.getCleanupDirectories(ctx)
		return err
	}); err != nil {
		return nil, err
	}
	return cleanupDirs, nil
}

func (o *snapshotter) getCleanupDirectories(ctx context.Context) ([]string, error) {
	ids, err := storage.IDMap(ctx)
	if err != nil {
		return nil, err
	}

	snapshotDir := filepath.Join(o.root, "snapshots")
	fd, err := os.Open(snapshotDir)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	dirs, err := fd.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	cleanup := []string{}
	for _, d := range dirs {
		if _, ok := ids[d]; ok {
			continue
		}
		cleanup = append(cleanup, filepath.Join(snapshotDir, d))
	}

	return cleanup, nil
}

func getActivePath(info *snapshots.Info, key string) (string, error) {
	if info.Labels != nil {
		if home, ok := info.Labels[snapshotters.ActiveSnapshotPathLabel]; ok {
			if !filepath.IsAbs(home) {
				return "", fmt.Errorf("path for active layer must be an absolute path: %w", errdefs.ErrInvalidArgument)
			}

			return filepath.Join(home, ".rwlayer"), nil
		}
	}

	return "", fmt.Errorf("active snapshot path is not defined")
}

func removeActivePath(info *snapshots.Info, key string) error {
	var removePath string

	if info.Labels == nil {
		return nil
	}

	if home, ok := info.Labels[snapshotters.ActiveSnapshotPathLabel]; ok {
		if key == "" {
			removePath = filepath.Join(home, ".rwlayer", info.Created.String())
			return os.RemoveAll(removePath)
		}
		keyList := append([]string{".rwlayer"}, strings.Split(key, "/")...)
		if err := os.RemoveAll(filepath.Join(home, ".rwlayer", key)); err != nil {
			return err
		}
		for i := len(keyList) - 1; i > 0; i-- {
			dirPath := strings.Join(keyList[0:i], "/")
			dir, _ := os.ReadDir(filepath.Join(home, dirPath))
			if len(dir) == 0 {
				os.RemoveAll(filepath.Join(home, dirPath))
			}
		}
	}

	return nil
}

func (o *snapshotter) createSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts []snapshots.Opt) (_ []mount.Mount, err error) {
	var (
		s                                    storage.Snapshot
		td, path, snapshotDir                string
		snapshotInfo                         snapshots.Info
		checkAlreadyExistSnapshotInActiveDir bool
	)

	for _, opt := range opts {
		if err := opt(&snapshotInfo); err != nil {
			return nil, err
		}
	}

	defer func() {
		if err != nil {
			if td != "" {
				if err1 := os.RemoveAll(td); err1 != nil {
					log.G(ctx).WithError(err1).Warn("failed to cleanup temp snapshot directory")
				}
			}
			if path != "" {
				if err1 := os.RemoveAll(path); err1 != nil {
					log.G(ctx).WithError(err1).WithField("path", path).Error("failed to reclaim snapshot directory, directory may need removal")
					err = fmt.Errorf("failed to remove path: %v: %w", err1, err)
				}
			}
		}
	}()

	if err := o.ms.WithTransaction(ctx, true, func(ctx context.Context) (err error) {
		if home, err := getActivePath(&snapshotInfo, key); err == nil {
			if _, err := os.Stat(home); err != nil {
				if err = os.MkdirAll(home, 0711); err != nil {
					return err
				}
			} else {
				logrus.Infof("active snapshot directory %s already exists", home)
			}

			snapshotDir = home
			checkAlreadyExistSnapshotInActiveDir = true
		} else {
			if !errdefs.IsNotFound(err) {
				log.G(ctx).WithError(err).Warn("invalid activePath specified")
			}

			snapshotDir = filepath.Join(o.root, "snapshots")
		}

		td, err = o.prepareDirectory(ctx, snapshotDir, kind, checkAlreadyExistSnapshotInActiveDir)
		if err != nil {
			return fmt.Errorf("failed to create prepare snapshot dir: %w", err)
		}

		if len(s.ParentIDs) > 0 {
			st, err := os.Stat(o.upperPath(nil, s.ParentIDs[0], key))

			if err != nil {
				return fmt.Errorf("failed to stat parent: %w", err)
			}

			stat := st.Sys().(*syscall.Stat_t)
			if err := os.Lchown(filepath.Join(td, "fs"), int(stat.Uid), int(stat.Gid)); err != nil {
				return fmt.Errorf("failed to chown: %w", err)
			}
		}

		s, err = storage.CreateSnapshot(ctx, kind, key, parent, opts...)
		if err != nil {
			return fmt.Errorf("failed to create snapshot: %w", err)
		}

		path = filepath.Join(snapshotDir, s.ID)

		if err = os.Rename(td, path); err != nil {
			return fmt.Errorf("failed to rename: %w", err)
		}
		td = ""

		return nil
	}); err != nil {
		return nil, err
	}

	return o.mounts(&snapshotInfo, s, key), nil
}

func (o *snapshotter) prepareDirectory(ctx context.Context, snapshotDir string, kind snapshots.Kind, checkSubfolder bool) (string, error) {
	prepareNewTempDirectory := func() (string, error) {
		td, err := os.MkdirTemp(snapshotDir, "new-")
		if err != nil {
			return "", fmt.Errorf("failed to create temp dir: %w", err)
		}

		if err := os.Mkdir(filepath.Join(td, "fs"), 0755); err != nil {
			return "", err
		}

		if kind == snapshots.KindActive {
			if err := os.Mkdir(filepath.Join(td, "work"), 0711); err != nil {
				return "", err
			}
		}

		return td, nil
	}

	if !checkSubfolder {
		return prepareNewTempDirectory()
	}

	// if this directory already has a subfolder with fs and work folders
	// then return this dir
	var folderName string
	err := filepath.Walk(snapshotDir, func(path string, info os.FileInfo, err error) error {
		logrus.Println("walking", info.Name(), "in", snapshotDir)
		if err != nil {
			return err
		}
		if info.IsDir() && info.Name() != filepath.Base(snapshotDir) {

			logrus.Println("found one contender while walking", info.Name(), "in", snapshotDir)
			// Check if the subfolder contains "work" and "fs" sub-subfolders
			workFolder := filepath.Join(path, "work")
			fsFolder := filepath.Join(path, "fs")
			if _, err := os.Stat(workFolder); err == nil {
				if _, err := os.Stat(fsFolder); err == nil {
					logrus.Println("found one contender while walking", info.Name(), "in ", snapshotDir)
					folderName = info.Name()
					return filepath.SkipDir // Stop walking subdirectories
				}
			}
		}
		return nil
	})

	if err != nil {
		return "", err
	}

	// else create temp dir and create fs and work dir in them and return
	if folderName == "" {
		return prepareNewTempDirectory()
	}

	return filepath.Join(snapshotDir, folderName), nil
}

func (o *snapshotter) mounts(info *snapshots.Info, s storage.Snapshot, key string) []mount.Mount {
	if len(s.ParentIDs) == 0 {
		// if we only have one layer/no parents then just return a bind mount as overlay
		// will not work
		roFlag := "rw"
		if s.Kind == snapshots.KindView {
			roFlag = "ro"
		}

		return []mount.Mount{
			{
				Source: o.upperPath(info, s.ID, key),
				Type:   "bind",
				Options: []string{
					roFlag,
					"rbind",
				},
			},
		}
	}

	options := o.options
	if s.Kind == snapshots.KindActive {
		options = append(options,
			fmt.Sprintf("workdir=%s", o.workPath(info, s.ID, key)),
			fmt.Sprintf("upperdir=%s", o.upperPath(info, s.ID, key)),
		)
	} else if len(s.ParentIDs) == 1 {
		return []mount.Mount{
			{
				Source: o.upperPath(info, s.ParentIDs[0], ""),
				Type:   "bind",
				Options: []string{
					"ro",
					"rbind",
				},
			},
		}
	}

	parentPaths := make([]string, len(s.ParentIDs))
	for i := range s.ParentIDs {
		parentPaths[i] = o.upperPath(nil, s.ParentIDs[i], "")
	}

	options = append(options, fmt.Sprintf("lowerdir=%s", strings.Join(parentPaths, ":")))
	return []mount.Mount{
		{
			Type:    "overlay",
			Source:  "overlay",
			Options: options,
		},
	}

}

func (o *snapshotter) upperPath(info *snapshots.Info, id string, key string) string {
	if info != nil {
		if home, err := getActivePath(info, key); err == nil {
			return filepath.Join(home, id, "fs")
		}
	}
	return filepath.Join(o.root, "snapshots", id, "fs")
}

func (o *snapshotter) workPath(info *snapshots.Info, id string, key string) string {
	if info != nil {
		if home, err := getActivePath(info, key); err == nil {
			return filepath.Join(home, id, "work")
		}
	}
	return filepath.Join(o.root, "snapshots", id, "work")
}

// Close closes the snapshotter
func (o *snapshotter) Close() error {
	return o.ms.Close()
}

// supportsIndex checks whether the "index=off" option is supported by the kernel.
func supportsIndex() bool {
	if _, err := os.Stat("/sys/module/overlay/parameters/index"); err == nil {
		return true
	}
	return false
}
