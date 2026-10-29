package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"

	"github.com/Dzarlax-AI/evening-news/go-tools/managedsetup"
	"golang.org/x/crypto/argon2"
	"golang.org/x/term"
)

func resetAdmin(ctx context.Context, l managedsetup.Layout) error {
	s, err := managedsetup.ReadState(l.Setup)
	if err != nil {
		return err
	}
	db, err := connectInstallerState(ctx, l, s)
	if err != nil {
		return err
	}
	defer db.Close()
	var exists bool
	if db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM news.managed_admin WHERE id=1)`).Scan(&exists) != nil || !exists {
		return errors.New("administrator does not exist; use first-install setup instead")
	}
	tty, err := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if err != nil {
		return errors.New("interactive terminal required for administrator reset")
	}
	defer tty.Close()
	fmt.Fprint(tty, "New administrator password (at least 12 characters): ")
	password, err := term.ReadPassword(int(tty.Fd()))
	fmt.Fprintln(tty)
	if err != nil || len(password) < 12 || len(password) > 1024 {
		return errors.New("invalid administrator password")
	}
	fmt.Fprint(tty, "Repeat password: ")
	again, err := term.ReadPassword(int(tty.Fd()))
	fmt.Fprintln(tty)
	if err != nil || string(password) != string(again) {
		return errors.New("passwords do not match")
	}
	salt := make([]byte, 16)
	if _, err = rand.Read(salt); err != nil {
		return errors.New("password reset unavailable")
	}
	hash := argon2.IDKey(password, salt, 2, 19456, 1, 32)
	encoded := fmt.Sprintf("$argon2id$v=19$m=19456,t=2,p=1$%s$%s", base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(hash))
	command, err := db.Exec(ctx, `UPDATE news.managed_admin SET password_hash=$1,revision=revision+1,updated_at=now() WHERE id=1`, encoded)
	if err != nil || command.RowsAffected() != 1 {
		return errors.New("password reset failed")
	}
	fmt.Println("Administrator password changed; previous sessions revoked")
	return nil
}
